const pipe = require('pipeline-pipe');
const { pipeline } = require('stream');
const JSONStream = require('JSONStream')
const { Transform } = require('stream');
const area = require('@mapbox/geojson-area').geometry;
const promclient = require('prom-client');
var utils = require('../utils/writer.js');

module.exports = {}

module.exports.successful_requests = new promclient.Counter({
	name: 'successful_requests',
	help: 'Number of requests that made it to the end of the processing pipeline, to be returned to the user',
	labelNames: ['endpoint', 'status_code'],
});

module.exports.request_error_counter = new promclient.Counter({
  name: 'request_errors',
  help: 'Number of failed requests',
  labelNames: ['endpoint', 'note'],
});

module.exports.queryCallback = function(postprocess, resolve, reject, err, data){
	// standard callback for a database query that should return an array, passed in as <data>.
	// <postprocess> == optional function to mutate <data> before return
	// <resolve> and <reject> == resolve and reject functions from a promise 
    if (err){
        console.log(err)
        reject({"code": 500, "message": "Server error"});
        return;
    }
    if(data.length == 0){
        reject({"code": 404, "message": "Not found: No matching results found in database."});
        return;
    }
    if(postprocess) resolve(postprocess(data))
    else resolve(data);
}

module.exports.validlonlat = function(shape, suppress){
    // shape: array of lon lat arrays, [[lon 0, lat 0], [lon 1, lat 1], [lon 2, lat 2]...]
    // returns the same array normalizd to longitudes on [-180,180] and latitudes on [-90,90]
    // unless suppress is set.

    if(suppress){
      return shape
    } else {
      return shape.map(([longitude, latitude]) => {
        long = longitude
        while (long > 180){
          long -= 360
        }
        while (long < -180){
          long += 360
        }

        lat = latitude
        while (lat > 90){
          lat -= 180
        }
        while (lat < -90){
          lat += 180
        }

        return [long, lat]
      })
    }
}

module.exports.polygon_sanitation = function(poly,enforceWinding,suppressCoordCleaning){
  // given a string <poly> that describes a polygon as [[lon0,lat0],[lon1,lat1],...,[lonN,latN],[lon0,lat0]],
  // make sure its formatted sensibly, and return it as a geojson polygon.
  const GJV = require('geojson-validation')
  let p = {}

  try {
    p = JSON.parse(poly);
  } catch (e) {
    return {"code": 400, "message": "Polygon region wasn't proper JSON; format should be [[lon,lat],[lon,lat],...]"};
  }

  p = module.exports.validlonlat(p, suppressCoordCleaning)

  p = {
    "type": "Polygon",
    "coordinates": [p]
  }

  if(enforceWinding){
    p["crs"] =  {
                  type: "name",
                  properties: { name: "urn:x-mongodb:crs:strictwinding:EPSG:4326" }
                }
  }

  if(!GJV.valid(p)){
    return {"code": 400, "message": "Polygon region wasn't proper geoJSON; format should be [[lon,lat],[lon,lat],...]"};
  }

  return p
}

module.exports.remove_laps = function(coordpairs){
  // if we imagine drawing shapes on a world map with centerlon in [-180,180],
  // there's no usecase for any coordinate pairs to ever be outside (-360, 360); remove extra full rotations.

  let extrarotations = 0
  let maxlong = Math.max(...coordpairs.map(subArray => subArray[0]))
  if(maxlong >= 360){
    extrarotations = Math.floor(maxlong / 360)
  }

  let minlong = Math.min(...coordpairs.map(subArray => subArray[0]))
  if(minlong <= -360){
    extrarotations = -1*Math.floor(-1*minlong / 360)
  }
  
  return coordpairs.map(x => [x[0] - 360*extrarotations, x[1]])
}

module.exports.box_sanitation = function(box,suppressCoordCleaning, suppressDatelineSplit){
  let b = {}

  try {
    b = JSON.parse(box);
  } catch (e) {
    return {"code": 400, "message": "Box region wasn't proper JSON; format should be [[lon,lat],[lon,lat]]"};
  }

  b = module.exports.remove_laps(b)

  // coerce into [-180,180]
  b = module.exports.validlonlat(b, suppressCoordCleaning)

  // might need to split up into two boxes if box crosses the dateline.
  // disambiguate by insisting that the first point is always west of the last point.
  if(b[0][0] > b[1][0] && !suppressDatelineSplit) {
    b = [
      [b[0],[180, b[1][1]]],    
      [[-180, b[0][1]],b[1]]    
    ]

  } else {
    b = [b]
  }

  return b.map(x => module.exports.validlonlat(x, suppressCoordCleaning))
}

module.exports.parameter_sanitization = function(dataset,id,startDate,endDate,polygon,box,winding,center,radius, suppressCoordCleaning){
  // sanity check and transform generic temporospatial query string parameters in preparation for search.

  params = {"dataset": dataset}

  if(id){
    params.id = String(id)
  }

  if(startDate){
    params.startDate = new Date(startDate);
  }

  if(endDate){
    params.endDate = new Date(endDate);
  }

  if(polygon){
    polygon = module.exports.polygon_sanitation(polygon, winding, suppressCoordCleaning)
    if(polygon.hasOwnProperty('code')){
      // error, return and bail out
      return polygon
    }
    params.polygon = polygon
  }

  if(box){
    box = module.exports.box_sanitation(box, suppressCoordCleaning)
    if(box.hasOwnProperty('code')){
      // error, return and bail out
      return box
    }
    params.box = box
  }

  if(center){
    params.center = center
  }

  if(radius){
    params.radius = radius
  }

  return params
}

module.exports.request_sanitation = function(polygon, center, radius, box, require_region, presRange, verticalRange){
  // given some parameters from a requst, decide whether or not to reject; return false == don't reject, return with message / code if do reject

  if(require_region && !polygon && !(center || radius) && !box){
    return {"code": 400, "message": "This route requires a geographic region, either a polygon, box, or center and radius."} 
  }

  // basic sanity checks
  if( (center && polygon) || (box && polygon) || (box && center)){
    return {"code": 400, "message": "Please request only one of polygon, box, or center."} 
  }
  if((center && !radius) || (!center && radius)){
    return {"code": 400, "message": "Please specify both radius and center to filter for data less than <radius> km from <center>."}
  }
  if(presRange && verticalRange){
    return {"code": 400, "message": "Please specify only one of presRange or verticalRange. Note that presRange is depricated; verticalRange will filter on pressure or depth, whichever is appropriate for this dataset."}
  }

  return false
}

module.exports.datatable_stream = function(model, params, local_filter, foreign_docs){
  // given <model>, a mongoose model pointing to a data collection,
  // <params> parameter kv with info for performing the selection and filtration
  // <local_filter> a custom set of aggregation pipeline steps to be applied to the data collection reffed by <model>,
  // and <foreign_docs>, an array of documents matching a query on the metadata collection which should constrain which data collection docs we return,
  // return a cursor over that which matches the above

  let spacetimeMatch = []
  let proxMatch = []
  let foreignMatch = []
  let isTimeseries = ['noaasst', 'copernicussla', 'ccmpwind'].includes(params.dataset)
  let geosearch = params.extended ? '$geoIntersects' : '$geoWithin' 

  // construct match stages as required
  /// prox match construction
  if(params.center && params.radius) {
    proxMatch.push({$geoNear: {key: 'geolocation', near: {type: "Point", coordinates: [params.center[0], params.center[1]]}, maxDistance: 1000*params.radius, distanceField: "distcalculated"}}) 
    proxMatch.push({ $unset: "distcalculated" })
  }
  /// spacetime match construction
  if(params.startDate || params.endDate || params.polygon || params.box){
    spacetimeMatch[0] = {$match: {}}
    if(!isTimeseries) {
      // time filtering at this stage only appropriate for point data
      if (params.startDate && params.endDate){
        spacetimeMatch[0]['$match']['timestamp'] = {$gte: params.startDate, $lt: params.endDate}
      } else if (params.startDate){
        spacetimeMatch[0]['$match']['timestamp'] = {$gte: params.startDate}
      } else if (params.endDate){
        spacetimeMatch[0]['$match']['timestamp'] = {$lt: params.endDate}
      }
    }
    if(params.polygon) {
      spacetimeMatch[0]['$match']['geolocation'] = {}
      spacetimeMatch[0]['$match']['geolocation'][geosearch] = {$geometry: params.polygon}
    }
    if(params.box) {
      // might have to $or over a list of two boxes to deal with the dateline
      let boxsearch = {'$or': params.box.map(b => ({'geolocation.coordinates': {'$geoWithin':{'$box':b}} })) }
      if(spacetimeMatch[0]['$match']['timestamp']){
        spacetimeMatch[0]['$match'] = {
          '$and': [
            boxsearch,
            {'timestamp': spacetimeMatch[0]['$match']['timestamp']}
          ]
        }
      } else {
        spacetimeMatch[0]['$match'] = boxsearch
      }
    }
  }

  /// construct filter for matching metadata docs if required; timeseries never filter on metadata docs
  if(!isTimeseries && params.metafilter){
    let metaIDs = new Set(foreign_docs.map(x => x['_id']))
    foreignMatch.push({$match:{'metadata':{$in:Array.from(metaIDs)}}})
  }

  // set up first part of pipeline aggregation:
  let aggPipeline = proxMatch.concat(spacetimeMatch).concat(local_filter).concat(foreignMatch)

  // construct transform stages as required

  /// data query filtration

  //// go find data_info on the metadata document and bring it along
  if(params.lookup_meta){
    aggPipeline.push({
      $lookup: {
        from: params.metacollection, 
        localField: 'metadata', 
        foreignField: '_id', 
        as: 'metadata_docs'
      }
    })

    // $lookup does not guarantee sort order, fix it - matters for merged grids for example
    aggPipeline.push({
      $addFields: {
        metadata_docs: {
          $function: {
            body: module.exports.sort_metadocs.toString(),
            args: ["$metadata", "$metadata_docs"],
            lang: 'js'
          }
        }
      }
    }) 

    aggPipeline.push({
      $addFields: {
        data_info: {
          $function: {
            body: module.exports.merge_data_info.toString(),
            args: ["$metadata_docs.data_info"],
            lang: 'js'
          }
        }
      }
    })    

    ///// if this is a grid, get the level data, too.
    if(params.is_grid){
      aggPipeline.push({
        $addFields: {
          levels: { $arrayElemAt: ["$metadata_docs.levels", 0] }
        }
      });
    }

    // aggPipeline.push({
    //   $project: {
    //     metadata_docs: 0
    //   }
    // });
  }

  //// some metadata documents pre-pulled have globally useful data
  if(params.archtypical_meta){

    if(params.is_timeseries){
      if(params.data_query){
        aggPipeline.push({
          $addFields: {
            data_info: foreign_docs[0].data_info
          }
        })
      }

      if(params.startDate || params.endDate){
        aggPipeline.push({
          $addFields: {
            timeseries: foreign_docs[0].timeseries
          }
        })
      }
    }

  }

  //// perform pressure filter
  if(params.verticalRange){
    // find the slice bounds
    aggPipeline.push({
      $addFields: {
        verticalRange: {
          $function: {
            body: module.exports.vertical_bounds.toString(),
            args: params.is_grid ? ["$data", "$data_info", params.verticalRange, "$levels"] : ["$data", "$data_info", params.verticalRange, null],
            lang: 'js'
          }
        }
      }
    })

    // do the slicing
    aggPipeline.push({
      $addFields: {
        data: {
          $function: {
            body: module.exports.vertical_data_slice.toString(),
            args: ["$data", "$verticalRange"],
            lang: 'js'
          }
        }
      }
    })

    if(params.is_grid){
      aggPipeline.push({
        $addFields: {
          levels: {
            $function: {
              body: module.exports.vertical_level_slice.toString(),
              args: ["$levels", "$verticalRange"],
              lang: 'js'
            }
          }
        }
      })
    }

    // drop the slice bounds
    aggPipeline.push({
      $project: {
        verticalRange: 0
      }
    });
  }

  // filter down to requested time range in mongo for timeseries data
  if(isTimeseries && (params.startDate || params.endDate)){

    // find the slice bounds
    let ts = JSON.parse(JSON.stringify(foreign_docs[0]['timeseries']))
    ts = ts.map(x => new Date(x))
    aggPipeline.push({
      $addFields: {
        timeRange: {
          $function: {
            body: module.exports.timerange_bounds.toString(),
            args: [ts, params.startDate, params.endDate],
            lang: 'js'
          }
        }
      }
    })

    // do the slicing
    aggPipeline.push({
      $addFields: {
        data: {
          $function: {
            body: module.exports.timeseries_data_slice.toString(),
            args: ["$data", "$timeRange"],
            lang: 'js'
          }
        }
      }
    })

    aggPipeline.push({
      $addFields: {
        timeseries: {
          $function: {
            body: module.exports.timeseries_slice.toString(),
            args: ["$timeseries", "$timeRange"],
            lang: 'js'
          }
        }
      }
    })

    // drop the slice bounds
    aggPipeline.push({
      $project: {
        timeRange: 0
      }
    });
  }

  if(params.data_query){
    //// if there's a requested measurement not present, or a negated measurement present, immediately null the data attribute for dropping
    aggPipeline.push({
      $addFields: {
        data: {
          $function: {
            body: module.exports.correct_data_available.toString(),
            args: [params.data_query, "$data", "$data_info"],
            lang: 'js'
          }
        }
      }
    })

    //// filter levels by QC requests
    if(params.qc_suffix){
      aggPipeline.push({
        $addFields: {
          data: {
            $function: {
              body: module.exports.qc_filter.toString(),
              args: [params.data_query, "$data", "$data_info", params.qc_suffix],
              lang: 'js'
            }
          }
        }
      })
    }

    //// find mask for .data, data_info[0], data_info[2]
    aggPipeline.push({
      $addFields: {
        data_mask: {
          $function: {
            body: module.exports.data_mask.toString(),
            args: [params.data_query, "$data", "$data_info", params.qc_suffix],
            lang: 'js'
          }
        }
      }
    })

    //// keep the data in the mask
    aggPipeline.push({
      $addFields: {
        data: {
          $function: {
            body: module.exports.data_filter.toString(),
            args: ["$data_mask", "$data", params.data_query],
            lang: 'js'
          }
        }
      }
    })

    //// keep the data_info in the mask
    aggPipeline.push({
      $addFields: {
        data_info: {
          $function: {
            body: module.exports.data_info_filter.toString(),
            args: ["$data_mask", "$data_info"],
            lang: 'js'
          }
        }
      }
    })

    //// dump the mask
    aggPipeline.push({
      $project: {
        data_mask: 0
      }
    });

    //// dump pressure if coerced and it's the only thing left on the data array after masking
    if(params.coerced_pressure){
      aggPipeline.push({
        $addFields: {
          data: {
            $function: {
              body: `function(data){
                if(data.length == 1){
                  return []
                } else {
                  return data
                }
              }`,
              args: ["$data"],
              lang: 'js'
            }
          }
        }
      })
    }
  }

  //// filter data for levels that have none of the requested data
  aggPipeline.push({
    $addFields: {
      data: {
        $function: {
          body: module.exports.level_filter.toString(),
          args: ["$data", "$data_info", params.coerced_pressure],
          lang: 'js'
        }
      }
    }
  })

  // if there's no data left after all that, drop the document
  aggPipeline.push({
    $match: {
      $expr: {
        $not: {
          $function: {
            body: module.exports.isArrayOfEmptyArrays.toString(),  
            args: ["$data"], 
            lang: "js"
          }
        }
      }
    }
  })

  if(!params.data_query || params.data_query[1].includes('except-data-values')){
    aggPipeline.push({
      $project: {
        data: 0
      }
    });
  }
  
  if(params.compression !== 'minimal'){
    // some stub requests are allowed that would swamp mongo's default sorting limits.
    aggPipeline.push({$sort: {'timestamp':-1}})
  }

  if(params.projection){
    // project out only the listed data document keys
    let project = {}
    for(let i=0;i<params.projection.length;i++){
      project[params.projection[i]] = 1
    }
    aggPipeline.push({$project: project})
  }

  if(params.junk){
    // drop junk parameters
    let junk = {}
    for(let i=0;i<params.junk.length;i++){
      junk[params.junk[i]] = 0
    }
    aggPipeline.push({$project: junk})
  }

  return model.aggregate(aggPipeline).cursor()  
}

module.exports.parse_data_qsp = function(data_query){
  let negated_variables = [];
  let flags = [];
  let variables = {};
  let allQcFlags = [];

  // Split the input string by commas and trim whitespace
  const tokens = data_query.split(',').map(token => token.trim());

  let currentVariable = null;

  tokens.forEach(token => {
    if (token.startsWith('~')) {
      // Add negated variable (removing the `~`)
      negated_variables.push(token.substring(1));
    } else if (token === 'all' || token === 'except-data-values') {
      // Handle 'all' flag - check if it has integers following it
      flags.push(token);
      currentVariable = token;  // Set the current variable to 'all'
    } else if (!isNaN(token) && currentVariable === 'all') {
      // If 'all' is followed by numbers, store these QC flags
      allQcFlags.push(Number(token));
    } else if (isNaN(token) || parseInt(token)>=90) { // argone forecasts use number of days as variable names...
      // It's a variable name, store it
      currentVariable = token;
      variables[currentVariable] = [];
    } else if (!isNaN(token) && currentVariable) {
      // If it's a number and we have a current variable, add the number to that variable's array
      variables[currentVariable].push(Number(token));
    }
  });

  return [negated_variables, flags, variables, allQcFlags];
}

module.exports.qc_filter = function (data_query, data, data_info, qc_suffix) {
  const variables = data_query[2];
  let allQcFlags = data_query[3];

  // Remove any variable that has an empty array - no QC filtering to do
  for (const key in variables) {
    if (variables[key].length === 0) {
      delete variables[key];
    }
  }

  // Apply 'all' QC flags to every variable that doesn't have specific QC values
  if (allQcFlags.length > 0) {
    data_info[0].forEach(field => {
      if (!field.endsWith(qc_suffix)) {  // Only apply to non-QC fields
        const varName = field;
        if (!(varName in variables)) {
          variables[varName] = allQcFlags.slice();  // Apply 'all' QC flags to this variable
        }
      }
    });
  }

  if (Object.keys(variables).length === 0) {
    // No variables to filter, return the data as is
    return data;
  }

  // Transpose the data so we can iterate through levels
  const transposedData = data[0].map((_, colIndex) => data.map(row => row[colIndex]));

  // Iterate over levels
  const updatedTransposedData = transposedData.map(lvl => {
    for (const key in variables) {
      const qcVariableName = key + qc_suffix;
      const qcIndex = data_info[0].indexOf(qcVariableName);
      const varIndex = data_info[0].indexOf(key);

      // If the QC variable is found and its value isn't in the allowed list, set the original value to null
      if (qcIndex !== -1 && !variables[key].includes(lvl[qcIndex])) {
        lvl[varIndex] = null;  // Set the value of the corresponding variable at this level to null
      }
    }
    return lvl;
  });

  // Un-transpose the data to return it back to the original row/column format
  const updatedData = updatedTransposedData[0]
    ? updatedTransposedData[0].map((_, rowIndex) => updatedTransposedData.map(col => col[rowIndex]))
    : [];

  return updatedData;
}

module.exports.data_mask = function(data_query, data, data_info, qc_suffix) {
  // returns the indexes of the variables to keep in data, data_info[0] and data_info[2]

  // Step 1: Return an empty array if any negated variable is found in labels
  for (const excludeVar of data_query[0]) {
    if (data_info[0].includes(excludeVar)) {
      return [];  // Return [] if a label is in the exclude list
    }
  }

  // Step 2: Return full data array if 'all' is in the list of variables to keep
  if (data_query[1].includes('all')) {
    return Array.from({ length: data.length}, (_, i) => i);
  }

  // Step 3: Initialize an array to store the indices of variables to keep
  const indices_to_keep = [];

  // Step 4: Find the indices of the variables to keep
  Object.keys(data_query[2]).forEach(varName => {
    const varIndex = data_info[0].indexOf(varName);
    if (varIndex !== -1) {
      indices_to_keep.push(varIndex);
    }
  });

  // make sure no indexes are duplicated
  return Array.from(new Set(indices_to_keep));
}

module.exports.sort_metadocs = function(metadata, metadata_docs){
  return metadata.map(id => metadata_docs.find(obj => obj._id === id))
}

module.exports.merge_data_info = function(di){

  let data_info = [[],[],[]]
  di.forEach(d => {
    data_info[0] = data_info[0].concat(d[0])
    data_info[1] = d[1]
    data_info[2] = data_info[2].concat(d[2])
  })

  return data_info
}

module.exports.data_filter = function(indexes, data, data_query) {
  if(data.length == 0){
    return data
  }

  let d = indexes.map(index => data[index]);

  if (!data_query[1].includes('all') && d.some(innerArray => innerArray.every(item => item === null))) { 
    // If all values for a variable are null and we specifically ask for it, return an empty array, QC failed
    return [];
  }

  return d;
}

module.exports.data_info_filter = function(indexes, data_info) {
  let d = [[], data_info[1], []];
  indexes.forEach(index => {
    d[0].push(data_info[0][index]);
    d[2].push(data_info[2][index]);
  });
  return d;
}

module.exports.vertical_bounds = function(data, data_info, verticalRange, levels) {
  let lvlSpectrum = null
  let pressure_index = data_info[0].findIndex(x => x === 'pressure')

  if(pressure_index !== -1){
    lvlSpectrum = data[pressure_index] // for profiles
  } else if(levels){
    lvlSpectrum = levels // for grids
  }
  
  if(lvlSpectrum){
    let lowIndex = 0
    let highIndex = lvlSpectrum.length-1

    if(lvlSpectrum[0] > verticalRange[1]){
      return [0,0]; // requested pressure range that is completely shallower than pressures available
    }
    if(lvlSpectrum[highIndex] < verticalRange[0]){
      return [0,0]; // requested pressure range that is completely deeper than pressures available
    }
    while(lowIndex < highIndex && lvlSpectrum[lowIndex] < verticalRange[0]){
      lowIndex++
    } // lowIndex now points at the first level index to keep
    while(highIndex > lowIndex && lvlSpectrum[highIndex] > verticalRange[1]){
      highIndex--
    } // highIndex now points at the last level index to keep

    return [lowIndex, highIndex+1]

  } else {
    return [0, 0]
  }
}

module.exports.vertical_data_slice = function(data, verticalRange){
  return data.map(lvl => lvl.slice(verticalRange[0], verticalRange[1]))
}

module.exports.vertical_level_slice = function(levels, verticalRange){
  return levels.slice(verticalRange[0], verticalRange[1])
}

module.exports.timerange_bounds = function(timeseries, startDate, endDate){
  let lowIndex = 0
  let highIndex = timeseries.length-1

  if(timeseries[0] > endDate){
    return [0,0]
  }
  if(timeseries[highIndex] < startDate){
    return [0,0]
  }
  while(lowIndex < highIndex && timeseries[lowIndex] < startDate){
    lowIndex++
  } // lowIndex now points at the first level index to keep
  while(highIndex > lowIndex && timeseries[highIndex] >= endDate){
    highIndex--
  } // highIndex now points at the last level index to keep

  return [lowIndex, highIndex+1]
}

module.exports.timeseries_data_slice = function(data, timeRange){
  return data.map(lvl => lvl.slice(timeRange[0], timeRange[1]))
}

module.exports.timeseries_slice = function(timeseries, timeRange){
  return timeseries.slice(timeRange[0], timeRange[1])
}

module.exports.level_filter = function(data, data_info, coerced_pressure){
  // drop levels that are all null for all requested variables

  // just pass through if no data
  if(data.length == 0){
    return data
  }

  // only want to consider non-coerced pressures for this filter
  filter_data = data.slice()

  if(coerced_pressure){
    let pressure_index = data_info[0].findIndex(x => x === 'pressure')
    filter_data.splice(pressure_index, 1);
  }

  const transposed_filter = filter_data[0].map((_, colIndex) => filter_data.map(row => row[colIndex]));

  const nullIndices = [];
  transposed_filter.forEach((innerArray, colIndex) => {
    if (innerArray.every(value => value === null)) {
      nullIndices.push(colIndex);
    }
  });

  const transposed_data = data[0].map((_, colIndex) => data.map(row => row[colIndex]));
  const filtered_transposed_data = transposed_data.filter((_, colIndex) => !nullIndices.includes(colIndex));

  const finalResult = filtered_transposed_data[0]
    ? filtered_transposed_data[0].map((_, rowIndex) => filtered_transposed_data.map(col => col[rowIndex]))
    : [];

  return finalResult;
}

module.exports.isArrayOfEmptyArrays = function(data) {
  // Return true if the data is an array of empty arrays, false otherwise
  if (data.length === 0) return true;  // Empty array is considered an array of empty arrays

  // Check if all elements are empty arrays
  return data.every(item => Array.isArray(item) && item.length === 0);
}

module.exports.correct_data_available = function(data_query, data, data_info) {
  // make sure that all requested data is present, and no negated data is present

  if (data_query[0].some(item => data_info[0].includes(item))){
    return []
  }

  if (!Object.keys(data_query[2]).every(item => data_info[0].includes(item))){
    return []
  }

  return data
}

module.exports.postprocess_stream = function(chunk, metadata, pp_params, stub){
  // <chunk>: raw data table document
  // <metadata>: metadata doc corresponding to this chunk
  // <pp_params>: kv with helpful information about what to do here
  // <stub>: function accepting one data document and its corresponding metadata document, returns appropriate representation for the compression=minimal flag.
  // returns chunk mutated into its final, user-facing form
  // or return false to drop this item from the stream (dont - drops should be in the agg pipeline)

  // return a minimal stub if requested
  if(pp_params.compression == 'minimal'){
    return stub(chunk, metadata)
  }

  return chunk
}

module.exports.post_xform = function(metaModel, pp_params, search_result, res, stub){

  let nDocs = 0
  let postprocess = pp_params.suppress_meta ? 
    pipe(async chunk => {
      // munge the chunk from the mongodb cursor and push it downstream if it isn't rejected.
      let doc = null
      if(!pp_params.mostrecent || nDocs < pp_params.mostrecent){
          /// ie dont even bother with post if we've exceeded our mostrecent cap
          doc = module.exports.postprocess_stream(chunk, [], pp_params, stub)
      }
      if(doc){
        if(!pp_params.mostrecent || nDocs < pp_params.mostrecent){
          res.status(200)
          nDocs++
          return(doc)
        }
      }
      return null
    }, 16) :
    pipe(async chunk => {
      // wait on a promise to get this chunk's metadata back
      meta = await module.exports.locate_meta(chunk['metadata'], search_result[0], metaModel)
      // keep track of new metadata docs so we don't look them up twice, and decide if we need to push them at the user in a batchmeta request
      let newmeta = []
      if(!pp_params.initialMetaPushComplete){
        newmeta = JSON.parse(JSON.stringify(search_result[0])) 
      }
      pp_params.initialMetaPushComplete = true
      for(let i=0; i<meta.length; i++){
        if(!search_result[0].find(x => x._id == meta[i]._id)){
          search_result[0].push(meta[i])
          newmeta.push(meta[i])
        } 
      }

      // hand back the metadata if it's new and that's what we want, OR munge the chunk and push it downstream if it isn't rejected.
      let doc = null
      if (pp_params.batchmeta && newmeta.length > 0 && (!pp_params.mostrecent || nDocs < pp_params.mostrecent)) {
        // hand back metadata
        res.status(200)
        nDocs += newmeta.length
        return(newmeta)        
      } else if(!pp_params.batchmeta && (!pp_params.mostrecent || nDocs < pp_params.mostrecent)){
          // munge the chunk and push it downstream if it isn't rejected.
          doc = module.exports.postprocess_stream(chunk, meta, pp_params, stub)
          if(doc){
            if(!pp_params.mostrecent || nDocs < pp_params.mostrecent){
              res.status(200)
              nDocs++
              return(doc)
            }
          }
      } else {
        return null
      }
    }, 16)

  return postprocess
}

module.exports.meta_xform = function(res){
  // transform stream that only looks for 404s

  let postprocess = pipe(async chunk => {
    res.status(200)
    return(chunk)
  })
    
  return postprocess
}

module.exports.locate_meta = function(meta_ids, meta_list, meta_model){
  // <meta_ids>: array of ids of meta documents of interest
  // <meta_list>: current array of fetched meta docs
  // <meta_model>: collection model to go looking in
  // return a promise that resolves to the metadata record sought.
  
  let current_meta = meta_list.map(x => x.metadata)
  current_meta = [].concat(...current_meta)
  meta_needed = meta_ids.filter(x => !current_meta.includes(x))

  if(meta_needed.length === 0){
    return new Promise(function(resolve, reject){resolve([])})
  } else {
    return meta_model.find({"_id": {"$in": meta_needed}}).lean().exec()
  }
}

module.exports.token_xform = function(res){
  // transform stream for token validation

  let postprocess = pipe(async chunk => {
    res.status(200)
    return({tokenValid: chunk.tokenValid > 0})
  })
 
  return postprocess
}

module.exports.lookup_key = function(userModel, apikey, resolve, reject){
    // look up an apikey from mongo, and reject if not found or not valid.

    if(process.env.ARGOCORE === 'here'){
      // look for the key in mongo
      const query = userModel.find({key: apikey})
      query.exec(function(err, user){
        if (err){
            reject({"code": 500, "message": "Server error"});
            return;
        }
        if(user.length == 0){
            reject({"code": 401, "message": "Invalid user key."});
            return;
        }
        if(!user[0].toObject().tokenValid){
          reject({"code": 403, "message": "API token has been deactivated; please contact argovis@colorado.edu for assistance."})
          return;
        }
        
        resolve(user[0].toObject())
      })
    } else{
      // phone home to validate this key
      fetch(process.env.ARGOCORE + '/token?token='+apikey, {headers:{'x-argokey': apikey}})
              .then(response => response.json())
              .then(data => {
                if(data.hasOwnProperty('code') && data.code == 401){
                  reject({"code": 401, "message": "Invalid user key."});
                  return;
                } else {
                  resolve({key: apikey, tokenValid: data[0].tokenValid})
                }
              })
    }
}

module.exports.earliest_records = function(dataset){
  // return a date representing the earliest record for the named dataset

  let dates = {
    'argo': new Date("1997-07-28T20:26:20.002Z"),
    'cchdo': new Date("1972-07-24T09:11:00Z"),
    'drifters': new Date("1987-10-02T13:00:00Z"),
    'kg21': new Date("2005-01-15T00:00:00Z"),
    'rg09': new Date("2004-01-15T00:00:00Z"),
    'tc': new Date("1851-06-25T00:00:00Z"),
    "trajectories": new Date("2001-01-04T22:46:33Z"),
    'noaasst': new Date("1989-12-31T00:00:00.000Z"),
    'copernicussla': new Date("1993-01-03T00:00:00Z"),
    'ccmpwind': new Date("1993-01-03T00:00:00Z"),
    'glodap': new Date('1000-01-01T00:00:00Z'),
    'ar': new Date("2000-01-01T00:00:00Z"),
    'easyocean': new Date("1983-10-08T00:00:00Z")
  }

  return dates[dataset]

}

module.exports.final_records = function(dataset){
  // return a date representing the last record for the named dataset, plus 1s
  // (used to coerce in an endDate when none provided, in which case we want to be inclusive of the last date as opposed to our usual exclusive, hence the +1s)

  let dates = {
    'argo': new Date(),
    'cchdo': new Date("2024-03-28T05:31:00Z"),
    'drifters': new Date("2020-06-30T23:00:01Z"),
    'kg21': new Date("2020-12-15T00:00:01Z"),
    'rg09': new Date("2022-05-15T00:00:01Z"),
    'tc': new Date("2020-12-25T12:00:01Z"),
    'trajectories': new Date("2021-01-01T01:13:27Z"),
    'noaasst': new Date("2023-01-29T00:00:01Z"),
    'copernicussla': new Date("2022-07-31T00:00:01Z"),
    'ccmpwind': new Date("2019-12-29T00:00:01Z"),
    'glodap': new Date('1000-01-01T00:00:01Z'),
    'ar': new Date("2022-01-01T00:00:01Z"),
    'easyocean': new Date("2022-10-16T00:00:01Z")
  }

  return dates[dataset]

}

module.exports.cost = function(url, c, cellprice, metaDiscount, maxbulk, maxbulk_timeseries){
  // return the tokenbucket price for this URL.
  // c == defualt cost
  // cellprice == token cost of 1 sq deg day
  // metaDiscount == scaledown factor to discount except-data-values request by relative to data requests
  // maxbulk == maximum allowed size of ndays x area[sq km]/13000sqkm; set to prevent OOM crashes
  // maxbulk_timeseries == maximum allowed size of area[sq km]/13000sqkm; set to prevent OOM crashes
  /// determine path steps

  let path = url.split('?')[0].replace(/\/+$/, "").replace(/\/+$/, "").split('/').slice(1)

  /// tokenize query string
  let qString = new URLSearchParams(url.split('?')[1]);

  /// handle standardized routes
  let standard_routes = ['argo', 'cchdo', 'drifters', 'tc', 'grids', 'trajectories', 'timeseries', 'extended', 'easyocean']

  if(standard_routes.includes(path[0])){
    //// metadata routes
    if(path.length==2 && path[1] == 'meta'){
      return c/metaDiscount
    }
    //// core data routes
    if(path.length==1 || (path[0]=='grids' && (path[1]=='rg09' || path[1]=='kg21' || path[1]=='glodap')) || (path[0]=='timeseries' && (path[1]=='noaasst' || path[1]=='copernicussla' || path[1]=='ccmpwind')) || (path[0]=='extended' && (path[1]=='ar')) ){
      ///// any query parameter that specifies a particular record or small set of records can get waived through
      if(qString.get('id') || qString.get('wmo') || qString.get('name')){
        c = c/5
      }
      //// query parameters that specify a larger but still circumscribed number of records
      else if(qString.get('woceline') || qString.get('cchdo_cruise') || qString.get('platform') || (!['grids', 'timeseries', 'extended'].includes(path[0]) && qString.get('metadata')) ){
        c = c/1
        if(url.includes('compression=minimal')) {
          c = c/5
        }
      }

      ///// assume a temporospatial query absent the above (and if _nothing_ is provided, assumes and rejects an all-space-and-time request)
      else{
        ///// parameter cleaning and coercing; don't coerce coords to be mongo appropriate here, causes problems with area computation
        let params = module.exports.parameter_sanitization(path[path.length-1], null,qString.get('startDate'),qString.get('endDate'),qString.get('polygon'),qString.get('box'),false,qString.get('center'),qString.get('radius'), true)
        if(params.hasOwnProperty('code')){
          return params
        }
        // request costs infer a startDate and endDate if not provided
        if(!(params.hasOwnProperty('startDate'))){
          params.startDate = module.exports.earliest_records(params['dataset'])
        }
        if(!(params.hasOwnProperty('endDate'))){
          params.endDate = module.exports.final_records(params['dataset'])
        }

        ///// cost out request; timeseries limited only by geography since entire time span for each matched lat/long must be pulled off disk in any case.
        let geospan = module.exports.geoarea(params.polygon,params.box,params.radius) / 13000 // 1 sq degree is about 13k sq km at eq
        let dayspan = Math.round(Math.abs((params.endDate - params.startDate) / (24*60*60*1000) )); // n days of request
        if((!url.includes('compression=minimal')) && (path[0]=='timeseries' && path.length==2 && geospan > maxbulk_timeseries) || (path[0]!='timeseries' && geospan*dayspan > maxbulk) ){
          return {"code": 413, "message": "The temporospatial extent of your request is very large and likely to crash our API. Please request a smaller region or shorter timespan, or both."}
        }
        if(path[0] == 'timeseries'){
          c = geospan*cellprice
        } else {
          c = geospan*dayspan*cellprice
        }
        if(isNaN(c)){
          c = 1 // protect against NaNs messing up user's token alotment
        }

        ///// metadata discount
        if(!url.includes('data') || url.includes('except-data-values') || url.includes('compression=minimal')){
          c /= metaDiscount
        }
      }
    } 
    //// */vocabulary routes unconstrained for now  
  } 

  /// all other routes unconstrained for now
  return c
}

module.exports.geoarea = function(polygon, box, radius){
  // return the area in sq km of the defined region

  let geospan = 360000000 // 360M sq km, all the oceans
  if(polygon){
      geospan = area(polygon) / 1000000
  } else if(radius){
      geospan = 3.14159*radius*radius // recall radius is reported in km
  } else if(box){
    // treat a box like a rectangular polygon
    geospan = 0
    for (let i=0; i<box.length; i++){
      geospan += area({"type":"Polygon", "coordinates":[[[box[i][0][0],box[i][0][1]],[box[i][1][0],box[i][0][1]],[box[i][1][0],box[i][1][1]],[box[i][0][0],box[i][1][1]],[box[i][0][0],box[i][0][1]]]]}) / 1000000
    }
  }

  return geospan
}

module.exports.data_pipeline = function(req, res, batchmeta, pipefittings){
  const flatten = new Transform({
    objectMode: true,
    transform(chunk, encoding, callback) {
      if (batchmeta && Array.isArray(chunk)) {
        chunk.forEach(item => this.push(item));
      } else {
        this.push(chunk);
      }
      callback();
    }
  });

  pipeline(
    ...pipefittings,
    flatten,
    JSONStream.stringify(),
    res.type('json'),
    (err) => {
      if(err){
        console.log(err)
      }
      module.exports.successful_requests.inc({ endpoint: req.path, status_code: res.statusCode });
    }
  )
}

module.exports.source_filter = function(sourcelist){
  // sourcelist: argument passed to `source` query string variable: comma separated string of sources, with ~negation
  // returns an aggregation stage reflecting negatable source.source searches

  let sourcematch = {}
  let smatches = sourcelist.filter(e => e.charAt(0)!='~')
  let snegations = sourcelist.filter(e => e.charAt(0)=='~').map(x => x.substring(1))
  if(smatches.length > 0 && snegations.length > 0){
    sourcematch['source.source'] = {'$all': smatches, '$nin': snegations}
  } else if (smatches.length > 0){
    sourcematch['source.source'] = {'$all': smatches}
  } else if (snegations.length > 0){
    sourcematch['source.source'] = {'$nin': snegations}
  }

  return {$match: sourcematch}
}

module.exports.find_grid_collection = function(token){
  // map a token including a grid's prefix ('rg09_temperature', 'rg09_salinity', ...) onto its collection name.

  if (["rg09_temperature", "rg09_salinity"].some(k => token.includes(k))) {
    return 'rg09'
  } else if(["kg21_ohc15to300"].some(k => token.includes(k))){
    return 'kg21'
  } else if (["glodap"].some(k => token.includes(k))){
    return 'glodap'
  } else {
    return ''
  }
}

module.exports.box2polygon = function(lowerLeft, upperRight) {
    let minLon = lowerLeft[0]
    let minLat = lowerLeft[1]
    let maxLon = upperRight[0]
    let maxLat = upperRight[1]

    if(maxLon < minLon){
      // ie if we've wrapped the dateline
      maxLon += 360
      maxLon = parseFloat(maxLon.toFixed(8)) // floating point errors can get really pathological here
    }

    const vertices = [];

    // Generate vertices along the bottom edge
    for (let lon = minLon; lon < maxLon; lon += 0.1) {
        vertices.push([lon, minLat]);
    }

    // Generate vertices along the right edge
    for (let lat = minLat; lat < maxLat; lat += 0.1) {
        vertices.push([maxLon, lat]);
    }

    // Generate vertices along the top edge
    for (let lon = maxLon; lon > minLon; lon -= 0.1) {
        vertices.push([lon, maxLat]);
    }

    // Generate vertices along the left edge
    for (let lat = maxLat; lat > minLat; lat -= 0.1) {
        vertices.push([minLon, lat]);
    }

    // Close the polygon by adding the first vertex again
    vertices.push([minLon, minLat]);

    const polygon = {
        type: "Polygon",
        coordinates: [vertices]
    };

    return polygon;
}

module.exports.lookupReject = function (req, res, response) {
  // generic function to handle when data lookup errors or rejects
  module.exports.request_error_counter.inc({ endpoint: req.path, note: 'data lookup fail' });
  utils.writeJson(res, response, response.code);
}

module.exports.catchPipeline = function (req, res, response) {
  // generic function to handle when pipeline rejects or errors
  module.exports.request_error_counter.inc({ endpoint: req.path, note: 'pipeline fail' });
  utils.writeJson(res, response);
}

module.exports.simpleWrite = function (req, res, response){
  // next thing in the promise chain after db lookup if we're skipping the fancy pipeline and just writing directly to the response
  module.exports.successful_requests.inc({ endpoint: req.path, status_code: res.statusCode });
  utils.writeJson(res, response);
}
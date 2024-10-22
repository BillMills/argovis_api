const request = require("supertest")("http://api:8080");
const expect = require("chai").expect;
const chai = require('chai');
chai.use(require('chai-json-schema'));
chai.use(require('chai-almost')(0.00000001));
const rawspec = require('/tests/core-spec.json');
const $RefParser = require("@apidevtools/json-schema-ref-parser");
const helpers = require('/tests/tests/helpers')

const c = 1
const cellprice = 0.0001
const metaDiscount = 100
const maxbulk = 1000000
const maxbulk_timeseries = 50
const bucketSize = 100

const dereferenceSchema = async (rawspec) => {
  return new Promise((resolve, reject) => {
    $RefParser.dereference(rawspec, (err, schema) => {
      if (err) {
        reject(err);
      } else {
        resolve(schema);
      }
    });
  });
};

let schema;

before(async function() {
  schema = await dereferenceSchema(rawspec);
});

describe("validlonlat", function () {
  it("waives through valid longitude", async function () {
    points = [[175,70],[177,56],[-155,56],[-154,69],[175,70]]
    expect(helpers.validlonlat(points)).to.eql([[175,70],[177,56],[-155,56],[-154,69],[175,70]])
  });
});

describe("validlonlat", function () {
  it("modulates an invalid longitude", async function () {
    points = [[-185,70],[-183,56],[-155,56],[-154,69],[-185,70]]
    expect(helpers.validlonlat(points)).to.eql([[175,70],[177,56],[-155,56],[-154,69],[175,70]])
  });
}); 

describe("validlonlat", function () {
  it("modulates an invalid longitude from several rotations away", async function () {
    points = [[-905,70],[-2703,56],[-155,56],[-154,69],[-905,70]]
    expect(helpers.validlonlat(points)).to.eql([[175,70],[177,56],[-155,56],[-154,69],[175,70]])
  });
}); 

describe("cost functions", function () {
  it("cost of entire globe for a day with data for a standard API route", async function () {
    expect(helpers.cost('/argo?startDate=2000-01-01T00:00:00Z&endDate=2000-01-02T00:00:00Z&data=doxy', c, cellprice, metaDiscount, maxbulk, maxbulk_timeseries)).to.almost.equal(360000000/13000*1*cellprice);
  });
}); 

describe("cost functions", function () {
  it("cost of entire globe for a day with data for a standard API route", async function () {
    expect(helpers.cost('/argo?startDate=2000-01-01T00:00:00Z&endDate=2000-01-02T00:00:00Z', c, cellprice, metaDiscount, maxbulk, maxbulk_timeseries)).to.almost.equal(360000000/13000*1*cellprice/metaDiscount);
  });
}); 

describe("cost functions", function () {
  it("cost of 15 deg box near equator for 2 years with data for a standard API route should prohibit more than a few such requests in parallel", async function () {
    expect(helpers.cost('/argo?startDate=2000-01-01T00:00:00Z&endDate=2002-01-01T00:00:00Z&polygon=[[0,-7.5],[15,-7.5],[15,7.5],[0,7.5],[0,-7.5]]&data=temperature', c, cellprice, metaDiscount, maxbulk, maxbulk_timeseries)*10).to.be.greaterThan(bucketSize);
  });
}); 

describe("cost functions", function () {
  it("cost of 15 deg box near equator for 30 years with data for a standard API route should be out of scope", async function () {
    expect(helpers.cost('/argo?startDate=2000-01-01T00:00:00Z&endDate=2030-01-01T00:00:00Z&polygon=[[0,-7.5],[15,-7.5],[15,7.5],[0,7.5],[0,-7.5]]&data=temperature', c, cellprice, metaDiscount, maxbulk, maxbulk_timeseries).code).to.eql(413);
  });
});

describe("cost functions", function () {
  it("cost of metadata request should be scaled by metaDiscount", async function () {
    expect(helpers.cost('/argo/meta?id=4901283_m0', c, cellprice, metaDiscount, maxbulk, maxbulk_timeseries)).to.eql(c/metaDiscount);
  });
}); 

describe("cost functions", function () {
  it("cost of metadata request should be scaled my metaDiscount", async function () {
    expect(helpers.cost('/grids/meta?id=kg21_ohc15to300', c, cellprice, metaDiscount, maxbulk, maxbulk_timeseries)).to.eql(c/metaDiscount);
  });
}); 

describe("grid prefixes", function () {
  it('checks mapping between grid names and collection names', async function () {
    expect(helpers.find_grid_collection('rg09_temperature_200401_Total')).to.eql('rg09');
  });
});

describe("GET /token", function () {
  it("check token fetching - valid", async function () {
    const response = await request.get("/token?token=guest").set({'x-argokey': 'developer'});
    expect(response.body[0]).to.deep.equal({'tokenValid': true})
  });
});

describe("GET /token", function () {
  it("check token fetching - invalid", async function () {
    const response = await request.get("/token?token=xxx").set({'x-argokey': 'developer'});
    expect(response.status).to.eql(404);
  });
});

describe("area functions", function () {
  it("should not overestimate the area of the north pacific above about 20N", async function () {
    expect(helpers.geoarea({'type':'Polygon', 'coordinates': [[[-110,22],[-238,20],[-230,32],[-218,36],[-214,45],[-200,54],[-193,60],[-157,58],[-146,61],[-124,47],[-120,37],[-110,22]]]})).to.be.lessThan(35000000);
  });
});

describe("box2polygon", function () {
  it("basic behavior of box2polygon", async function () {
    box = [[0,0],[0.15,0.15]]
    expect(helpers.box2polygon(box[0], box[1])).to.almost.deep.equal({'type':'Polygon', 'coordinates':[[[0,0],[0.1,0],[0.15,0],[0.15,0.1],[0.15,0.15],[0.05,0.15],[0,0.15],[0,0.05],[0,0]]]})
  });
}); 

describe("box2polygon", function () {
  it("box2polygon over the dateline", async function () {
    box = [[179.9,0],[-179.9,0.1]]
    expect(helpers.box2polygon(box[0], box[1])).to.almost.deep.equal({'type':'Polygon', 'coordinates':[[ [179.9,0],[180,0],[180.1,0],[180.1,0.1],[180,0.1],[179.9,0.1],[179.9,0] ]]})
  });
}); 

describe("box2polygon", function () {
  it("box2polygon over the dateline, one cycle back", async function () {
    box = [[-180.1,0],[-539.9,0.1]]
    expect(helpers.box2polygon(box[0], box[1])).to.almost.deep.equal({'type':'Polygon', 'coordinates':[[ [-180.1,0],[-180,0],[-179.9,0],[-179.9,0.1],[-180,0.1],[-180.1,0.1],[-180.1,0] ]]})
  });
}); 

describe("box_sanitation", function () {
  it("handle boxes that cross the dateline", async function () {
    box = '[[175,0],[-175,5]]'
    expect(helpers.box_sanitation(box)).to.almost.deep.equal([ [[175,0],[180,5]], [[-180,0],[-175,5]] ])
  });
});

describe("box_sanitation", function () {
  it("handle boxes that cross -180 on (-360, 360)", async function () {
    box = '[[-185,0],[-175,5]]'
    expect(helpers.box_sanitation(box)).to.almost.deep.equal([ [[175,0],[180,5]], [[-180,0],[-175,5]] ])
  });
}); 

describe("box_sanitation", function () {
  it("handle boxes that cross 180 on (-360, 360)", async function () {
    box = '[[175,0],[185,5]]'
    expect(helpers.box_sanitation(box)).to.almost.deep.equal([ [[175,0],[180,5]], [[-180,0],[-175,5]] ])
  });
}); 

describe("box_sanitation", function () {
  it("handle boxes multiple rotations away", async function () {
    box = '[[895,0],[545,5]]'
    expect(helpers.box_sanitation(box)).to.almost.deep.equal([ [[175,0],[180,5]], [[-180,0],[-175,5]] ])
  });
});

describe("box_sanitation", function () {
  it("handle boxes multiple negative rotations away", async function () {
    box = '[[-545,0],[-895,5]]'
    expect(helpers.box_sanitation(box)).to.almost.deep.equal([ [[175,0],[180,5]], [[-180,0],[-175,5]] ])
  });
}); 

describe("box_sanitation", function () {
  it("waive through boxes that dont cross the dateline", async function () {
    box = '[[-175,0],[175,5]]'
    expect(helpers.box_sanitation(box)).to.almost.deep.equal([ [[-175,0],[175,5]] ])
  });
}); 

describe("remove_laps", function () {
  it("do nothing with no laps to remove", async function () {
    box = [[0,0],[10,5]]
    expect(helpers.remove_laps(box)).to.almost.deep.equal([[0,0],[10,5]])
  });
}); 

describe("remove_laps", function () {
  it("remove negative laps", async function () {
    box = [[-720,0],[-710,5]]
    expect(helpers.remove_laps(box)).to.almost.deep.equal([[0,0],[10,5]])
  });
});

describe("remove_laps", function () {
  it("remove positive laps", async function () {
    box = [[720,0],[730,5]]
    expect(helpers.remove_laps(box)).to.almost.deep.equal([[0,0],[10,5]])
  });
}); 

describe("qc_filter", function () {
  it("check basic behavior of qc filter", async function () {
    data_query = helpers.parse_data_qsp('temp,doxy,1,4')
    data = [[1,2,3], [10,20,30], [2,2,2], [1,3,4]]
    data_info = [['temp', 'doxy', 'temp_argoqc', 'doxy_argoqc']]
    qc_suffix = '_argoqc'
    expect(helpers.qc_filter(data_query, data, data_info, qc_suffix)).to.deep.equal([[1,2,3], [10,null,30], [2,2,2], [1,3,4]])
  });
}); 

describe("qc_filter", function () {
  it("qc filter - no qc conditions", async function () {
    data_query = helpers.parse_data_qsp('temp,doxy')
    data = [[1,2,3], [10,20,30], [2,2,2], [1,3,4] ]
    data_info = [['temp', 'doxy', 'temp_qc', 'doxy_qc']]
    qc_suffix = '_qc'
    expect(helpers.qc_filter(data_query, data, data_info, qc_suffix)).to.deep.equal([[1,2,3], [10,20,30], [2,2,2], [1,3,4]])
  });
}); 

describe("qc_filter", function () {
  it("qc filter - all", async function () {
    data_query = helpers.parse_data_qsp('all,1')
    data = [[1,2,3], [10,20,30], [1,2,2], [1,3,4] ]
    data_info = [['temp', 'doxy', 'temp_qc', 'doxy_qc']]
    qc_suffix = '_qc'
    expect(helpers.qc_filter(data_query, data, data_info, qc_suffix)).to.deep.equal([[1,null,null], [10,null,null], [1,2,2], [1,3,4]])
  });
}); 

describe("data_mask", function () {
  it("data mask - negations", async function () {
    data_query = helpers.parse_data_qsp('temp,~doxy')
    data = [[1,2,3], [10,20,30], [1,2,2], [1,3,4] ]
    data_info = [['temp', 'doxy', 'temp_qc', 'doxy_qc']]
    qc_suffix = '_qc'
    expect(helpers.data_mask(data_query, data, data_info, qc_suffix)).to.deep.equal([])
  });
}); 

describe("data_mask", function () {
  it("data mask - all", async function () {
    data_query = helpers.parse_data_qsp('temp,all')
    data = [[1,2,3], [10,20,30], [1,2,2], [1,3,4]]
    data_info = [['temp', 'doxy', 'temp_qc', 'doxy_qc']]
    qc_suffix = '_qc'
    expect(helpers.data_mask(data_query, data, data_info, qc_suffix)).to.deep.equal([0,1,2,3])
  });
}); 

describe("data_mask", function () {
  it("data mask - nominal", async function () {
    data_query = helpers.parse_data_qsp('temp')
    data = [[1,2,3], [10,20,30], [1,2,2], [1,3,4]]
    data_info = [['temp', 'doxy', 'temp_qc', 'doxy_qc']]
    qc_suffix = '_qc'
    expect(helpers.data_mask(data_query, data, data_info, qc_suffix)).to.deep.equal([0])
  });
}); 

describe("vertical_bounds", function () {
  it("vertical filter - nominal", async function () {
    doc = {
      data: [[0,0,0,0,0,0], [1,2,3,4,5,6]],
      data_info: [['temp', 'pressure']]
    }
    verticalRange = [2.3,4.5]
    expect(helpers.vertical_bounds(doc.data, doc.data_info, verticalRange)).to.deep.equal([2,4])
  });
}); 

describe("level_filter", function () {
  it("level filter - nominal", async function () {
    data = [[0,null,0,0,0,0], [1,2,3,4,5,6]]
    data_info = [['temp', 'pressure']]
    coerced_pressure = true
    expect(helpers.level_filter(data, data_info, coerced_pressure)).to.deep.equal([[0,0,0,0,0], [1,3,4,5,6]])
  });
});

describe("sort_metadocs", function () {
  it("sort metadata documents - nominal", async function () {
    metadata = ['A', 'B']
    metadocs = [{_id: 'B'}, {_id: 'A'}]
    expect(helpers.sort_metadocs(metadata, metadocs)).to.deep.equal([{_id: 'A'}, {_id: 'B'}])
  });
});
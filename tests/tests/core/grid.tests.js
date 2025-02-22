const request = require("supertest")("http://api:8080");
const expect = require("chai").expect;
const chai = require('chai');
chai.use(require('chai-json-schema'));
const rawspec = require('/tests/core-spec.json');
const $RefParser = require("@apidevtools/json-schema-ref-parser");

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

describe("GET /grids/rg09", function () {
  it("searches for grids, dont request data", async function () {
    const response = await request.get("/grids/rg09?polygon=[[20,-64],[21,-64],[21,-65],[20,-65],[20,-64]]").set({'x-argokey': 'developer'});
    expect(response.body).to.be.jsonSchema(schema.paths['/grids/{gridName}'].get.responses['200'].content['application/json'].schema);
  });
});

describe("GET /grids/rg09", function () {
  it("searches for grids profiles with data=rg09_salinity,except-data-values", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_28.5_-64.5&data=rg09_salinity,except-data-values").set({'x-argokey': 'developer'});
    expect(response.body).to.be.jsonSchema(schema.paths['/grids/{gridName}'].get.responses['200'].content['application/json'].schema);
  });
});

describe("GET /grids/rg09", function () {
  it("reject profiles via with data=~rg09_salinity", async function () {
    const response = await request.get("/grids/rg09?polygon=[[20,-64],[21,-64],[21,-65],[20,-65],[20,-64]]&data=~rg09_salinity").set({'x-argokey': 'developer'});
    expect(response.status).to.eql(404);
  });
});

describe("GET /grids/rg09", function () {
  it("searches for grids profiles with data=except-data-values, should be the same as omitting the data key", async function () {
    const response1 = await request.get("/grids/rg09?polygon=[[20,-64],[21,-64],[21,-65],[20,-65],[20,-64]]&data=except-data-values").set({'x-argokey': 'developer'});
    const response2 = await request.get("/grids/rg09?polygon=[[20,-64],[21,-64],[21,-65],[20,-65],[20,-64]]").set({'x-argokey': 'developer'});
    expect(response1.body).to.eql(response2.body)
  });
});

describe("GET /grids/rg09", function () {
  it("grids with data filter should return grids-consistent data", async function () {
    const response = await request.get("/grids/rg09?polygon=[[20,-64],[21,-64],[21,-65],[20,-65],[20,-64]]&data=rg09_temperature").set({'x-argokey': 'developer'});
    expect(response.body).to.be.jsonSchema(schema.paths['/grids/{gridName}'].get.responses['200'].content['application/json'].schema);
  });
});

describe("GET /grids/rg09", function () {
  it("check correct array packing - temperature", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_29.5_-64.5&data=rg09_temperature").set({'x-argokey': 'developer'});
    expect(response.body[0].data[0]).to.deep.eql([ 0.035, -0.017, -0.156, -0.534, -1.112, -1.45, -1.545, -1.509, -1.284, -0.925, -0.491, -0.067, 0.37, 0.705, 0.94, 1.108, 1.22, 1.269, 1.328, 1.394, 1.417, 1.422, 1.425, 1.429, 1.426, 1.418, 1.404, 1.387, 1.376, 1.371, 1.352, 1.334, 1.308, 1.262, 1.205, 1.15, 1.089, 1.029, 0.972, 0.918, 0.867, 0.815, 0.771, 0.728, 0.691, 0.653, 0.621, 0.585, 0.555, 0.529, 0.502, 0.468, 0.433, 0.384, 0.335, 0.288, 0.238, 0.209 ])
  });
});

describe("GET /grids/rg09", function () {
  it("check correct array packing - salinity", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_29.5_-64.5&data=rg09_salinity").set({'x-argokey': 'developer'});
    expect(response.body[0].data[0]).to.deep.eql([ 33.702, 33.718998, 33.767002, 33.876999, 34.02, 34.110001, 34.160999, 34.208, 34.261002, 34.318001, 34.376999, 34.435001, 34.490002, 34.532001, 34.563, 34.587997, 34.606998, 34.618, 34.632999, 34.651001, 34.664001, 34.674, 34.681, 34.688999, 34.694, 34.699001, 34.702, 34.704998, 34.711002, 34.707001, 34.709003, 34.711002, 34.712002, 34.711998, 34.712997, 34.712997, 34.710999, 34.709, 34.707001, 34.705002, 34.702999, 34.701, 34.698002, 34.696999, 34.695, 34.694, 34.693001, 34.691002, 34.689999, 34.689999, 34.688999, 34.688, 34.687, 34.683998, 34.681, 34.679001, 34.675999, 34.674999 ] )
  });
});

describe("GET /grids/rg09", function () {
  it("grids with data filter should return correct data_keys", async function () {
    const response = await request.get("/grids/rg09?polygon=[[20,-64],[21,-64],[21,-65],[20,-65],[20,-64]]&data=rg09_temperature").set({'x-argokey': 'developer'});
    expect(response.body[0].data_info[0]).to.have.members(['rg09_temperature'])
  });
});

describe("GET /grids/rg09", function () {
  it("grids with data filter should return correct units", async function () {
    const response = await request.get("/grids/rg09?polygon=[[20,-64],[21,-64],[21,-65],[20,-65],[20,-64]]&data=rg09_temperature").set({'x-argokey': 'developer'});
    expect(response.body[0].data_info[2]).to.deep.equal([["degree celcius (ITS-90)"]])
  });
});

describe("GET /grids/rg09", function () {
  it("grids with data=all filter should return grids-consistent data", async function () {
    const response = await request.get("/grids/rg09?polygon=[[20,-64],[21,-64],[21,-65],[20,-65],[20,-64]]&data=all").set({'x-argokey': 'developer'});
    expect(response.body).to.be.jsonSchema(schema.paths['/grids/{gridName}'].get.responses['200'].content['application/json'].schema);
  });
});

describe("GET /grids/rg09", function () {
  it("grids profile should be dropped if no requested data is available", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_20.5_-64.5&data=kg21_ohc15to300").set({'x-argokey': 'developer'});
    expect(response.status).to.eql(404);
  });
});

describe("GET /grids/rg09", function () {
  it("grids profile should be dropped if grid is present but has no requested levels (presRange too high)", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_20.5_-64.5&data=rg09_salinity&presRange=2000,3000").set({'x-argokey': 'developer'});
    expect(response.status).to.eql(404);
  });
});

describe("GET /grids/rg09", function () {
  it("grids profile should be dropped if grid is present but has no requested levels (verticalRange too high); verticalRange alias", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_20.5_-64.5&data=rg09_salinity&verticalRange=2000,3000").set({'x-argokey': 'developer'});
    expect(response.status).to.eql(404);
  });
});

describe("GET /grids/rg09", function () {
  it("grids profile should be dropped if grid is present but has no requested levels (presRange too low)", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_20.5_-64.5&data=rg09_salinity&presRange=0,1").set({'x-argokey': 'developer'});
    expect(response.status).to.eql(404);
  });
});

describe("GET /grids/rg09", function () {
  it("presRange just intersecting top of range", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_39.5_-64.5&data=rg09_salinity&presRange=0,2.5").set({'x-argokey': 'developer'});
    expect(response.body[0].data).to.eql([[33.732002]]);
  });
});

describe("GET /grids/rg09", function () {
  it("presRange just intersecting bottom of range", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_39.5_-64.5&data=rg09_salinity&presRange=1975,2000").set({'x-argokey': 'developer'});
    expect(response.body[0].data).to.eql([[34.679001]]);
  });
});

describe("GET /grids/rg09", function () {
  it("presRange is exclusive on boundaries", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_39.5_-64.5&data=rg09_salinity&presRange=0,170").set({'x-argokey': 'developer'});
    expect(response.body[0].levels).to.eql([2.5, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 110, 120, 130, 140, 150, 160]);
  });
});

describe("GET /grids/rg09", function () {
  it("grids with data=all and presRange filters should return correct data", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_20.5_-64.5&data=all&presRange=5,100").set({'x-argokey': 'developer'});
    tempIndex = 0
    while(!response.body[0].metadata[tempIndex].includes('rg09_temperature')){
      tempIndex++
    }
    expect(response.body[0].data[tempIndex][0]).to.eql(-0.076)
  });
});

describe("GET /grids/meta", function () {
  it("grid metadata", async function () {
    const response = await request.get("/grids/meta?id=rg09_temperature_200401_Total").set({'x-argokey': 'developer'});
    expect(response.body).to.be.jsonSchema(schema.paths['/grids/meta'].get.responses['200'].content['application/json'].schema);
  });
}); 

describe("GET /grids/meta", function () {
  it("grid metadata 404", async function () {
    const response = await request.get("/grids/meta?id=xxx").set({'x-argokey': 'developer'});
    expect(response.status).to.eql(404);
    expect(response.body).to.eql({code: 404,message: "No grid product matching ID xxx"});
  });
}); 

describe("GET /grids/rg09", function () {
  it("fetch gridded data with pressure bracket", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_20.5_-64.5&presRange=10,100&data=rg09_temperature").set({'x-argokey': 'developer'});
    expect(response.body[0]['data']).to.eql([[-0.076, -0.21, -0.593, -1.201, -1.598, -1.663, -1.569, -1.15, -0.603]]);
  });
});

describe("GET /grids/rg09", function () {
  it("check reported level range with pressure bracket", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_20.5_-64.5&presRange=10,100&data=rg09_temperature").set({'x-argokey': 'developer'});
    expect(response.body[0]['levels']).to.eql([10,20,30,40,50,60,70,80,90]);
  });
});

describe("GET /grids/rg09", function () {
  it("should return appropriate minimal representation of this measurement", async function () {
    const response = await request.get("/grids/rg09?id=20040115000000_20.5_-64.5&compression=minimal").set({'x-argokey': 'developer'});
    expect(response.body).to.eql([['20040115000000_20.5_-64.5', 20.5, -64.5, "2004-01-15T00:00:00.000Z", ["rg09_temperature_200401_Total","rg09_salinity_200401_Total"]]]);  
  });
}); 

describe("GET /grids/rg09", function () {
  it("should work properly when limiting presRange and returning multiple matches", async function () {
    const response = await request.get("/grids/rg09?polygon=[[22,-65],[22,-64],[26,-64],[26,-65],[22,-65]]&startDate=2000-01-01T00:00:00Z&endDate=2020-01-01T00:00:00Z&data=rg09_temperature&presRange=0,100").set({'x-argokey': 'developer'});
    expect(response.body).to.be.jsonSchema(schema.paths['/grids/{gridName}'].get.responses['200'].content['application/json'].schema);
  });
});

describe("GET /grids/rg09", function () {
  it("check basic box behavior", async function () {
    const response = await request.get("/grids/rg09?box=[[20,-65],[21,-64]]").set({'x-argokey': 'developer'});
    expect(response.body.length).to.eql(1); 
  });
}); 

describe("GET /grids/glodap", function () {
  it("glodap should accept a verticalRange filter", async function () {
    const response = await request.get("/grids/glodap?id=20.5_-70.5&verticalRange=0,100").set({'x-argokey': 'developer'});
    expect(response.body.length).to.eql(1); 
  });
});

describe("GET /grids/glodap", function () {
  it("glodap should not accept a presRange filter", async function () {
    const response = await request.get("/grids/glodap?id=20.5_-70.5&presRange=0,100").set({'x-argokey': 'developer'});
    expect(response.status).to.eql(400);
  });
});
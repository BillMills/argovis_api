const request = require("supertest")("http://api:8080");
const expect = require("chai").expect;
const chai = require('chai');
chai.use(require('chai-json-schema'));
const rawspec = require('/tests/spec.json');
const $RefParser = require("@apidevtools/json-schema-ref-parser");

$RefParser.dereference(rawspec, (err, schema) => {
  if (err) {
    console.error(err);
  }
  else {
    describe("GET /ohc_kg", function () {
      it("fetch gridded data", async function () {
        const response = await request.get("/ohc_kg?polygon=[[112,-65],[112,-64],[114,-64],[114,-65],[112,-65]]&startDate=2000-01-01T00:00:00Z&endDate=2020-01-01T00:00:00Z&data=ohc_kg").set({'x-argokey': 'developer'});
        expect(response.body).to.be.jsonSchema(schema.paths['/ohc_kg'].get.responses['200'].content['application/json'].schema);
      });
    });

    describe("GET /temperature_rg", function () {
      it("fetch gridded data with pressure bracket", async function () {
        const response = await request.get("/temperature_rg?id=20190115000000_20.5_-64.5&presRange=50,100").set({'x-argokey': 'developer'});
        expect(response.body[0]['data']).to.eql( [[ -1.37 ], [ -1.508 ], [ -1.468 ], [ -1.206 ], [ -0.745 ], [ -0.275 ]]);
      });
    });

    describe("GET /ohc_kg", function () {
      it("fetch gridded data in overlap region between two polygons", async function () {
        const response = await request.get("/ohc_kg?multipolygon=[[[112,-65],[112,-64],[116,-64],[116,-65],[112,-65]],[[114,-65],[114,-64],[120,-64],[120,-65],[114,-65]]]&startDate=2000-01-01T00:00:00Z&endDate=2020-01-01T00:00:00Z").set({'x-argokey': 'developer'});
        expect(response.body.length).to.eql(2);
      });
    });

    describe("GET /ohc_kg", function () {
      it("reject a huge request", async function () {
        const response = await request.get("/ohc_kg?startDate=2020-01-01T00:00:00Z&endDate=2021-01-01T00:00:00Z&polygon=[[0,-30],[60,-30],[60,30],[0,30],[0,-30]]").set({'x-argokey': 'developer'});
        expect(response.status).to.eql(413);
      });
    });
  }
})


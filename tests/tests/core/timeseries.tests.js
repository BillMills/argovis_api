const request = require("supertest")("http://api:8080");
const expect = require("chai").expect;
const chai = require('chai');
chai.use(require('chai-json-schema'));
const rawspec = require('/tests/core-spec.json');
const $RefParser = require("@apidevtools/json-schema-ref-parser");

$RefParser.dereference(rawspec, (err, schema) => {
  if (err) {
    console.error(err);
  }
  else {
    
    describe("GET /noaasst", function () {
      it("make sure sst time slicing matches expectations", async function () {
        const response = await request.get("/noaasst?center=-46.5,35.5&radius=1&startDate=1989-12-31T00:00:00Z&endDate=1990-01-28T00:00:00Z&data=all").set({'x-argokey': 'developer'});
        expect(response.body[0].data).to.eql([[19.330000000000002,19.330000000000002,19.73,19.330000000000002]])        
      });
    });

    describe("GET /copernicussla", function () {
      it("make sure sla time slicing matches expectations", async function () {
        const response = await request.get("/copernicussla?center=-46.875,35.625&radius=1&startDate=1993-01-30T00:00:00Z&endDate=1993-02-14T00:00:00Z&data=all").set({'x-argokey': 'developer'});
        expect(response.body[0].data).to.eql([[-0.15601428571428572,-0.066],[0.2496428571428571,0.3396428571428572]])        
      });
    });

    describe("GET /noaasst", function () {
      it("allow noaa sst id request; shouldn't have timeseries appended", async function () {
        const response = await request.get("/noaasst?id=-46.5_35.5&data=all").set({'x-argokey': 'developer'});
        expect(response.status).to.eql(200);
        expect(response.body).to.be.jsonSchema(schema.paths['/noaasst'].get.responses['200'].content['application/json'].schema); 
        expect(response.body[0]).not.to.have.property('timeseries')  
        expect(response.body[0].data[0].length).to.eql(1727)   
      });
    });

    describe("GET /copernicussla", function () {
      it("allow copernicus sla id request; shouldn't have timeseries appended", async function () {
        const response = await request.get("/copernicussla?id=-46.875_35.625&data=all").set({'x-argokey': 'developer'});
        expect(response.status).to.eql(200);
        expect(response.body).to.be.jsonSchema(schema.paths['/copernicussla'].get.responses['200'].content['application/json'].schema);
        expect(response.body[0]).not.to.have.property('timeseries')
        expect(response.body[0].data[0].length).to.eql(1543) 
      });
    });

  }
})





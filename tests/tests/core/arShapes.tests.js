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
    describe("GET /extended/ar?_id", function () {
      it("returns one atmo river result by id", async function () {
        const response = await request.get("/extended/ar?id=2000.01.01.03.0_5").set({'x-argokey': 'developer'});
        expect(response.body).to.be.jsonSchema(schema.paths['/extended/{extendedName}'].get.responses['200'].content['application/json'].schema);
      });
    });

    describe("GET /extended/ar?polygon", function () {
      it("returns one atmo river result by geography", async function () {
        const response = await request.get("/extended/ar?polygon=[[-31.56195028862129,-40.41274267694427],[-30.162402425953275,-48.656846368839986],[-19.85493033767642,-47.494728836010914],[-23.12999463207592,-39.42714908325042],[-31.56195028862129,-40.41274267694427]]").set({'x-argokey': 'developer'});
        expect(response.body).to.be.jsonSchema(schema.paths['/extended/{extendedName}'].get.responses['200'].content['application/json'].schema);
        expect(response.body.length).to.eql(1)
        expect(response.body[0]['_id']).to.eql('2000.01.01.03.0_5')
      });
    });

    describe("GET /extended/ar", function () {
      it("check basic box behavior", async function () {
        const response = await request.get("/extended/ar?box=[[-35,-55],[-34,-54]]").set({'x-argokey': 'developer'});
        expect(response.body.length).to.eql(1); 
      });
    }); 

    describe("GET /extended/ar", function () {
      it("check box behavior over dateline", async function () {
        const response_dl = await request.get("/extended/ar?box=[[179,-55],[-179,-54]]").set({'x-argokey': 'developer'});
        expect(response_dl.body.length).to.eql(0); 
        const response_nodl = await request.get("/extended/ar?box=[[-179,-55],[179,-54]]").set({'x-argokey': 'developer'});
        expect(response_nodl.body.length).to.eql(1); 
      });
    });

    describe("GET /extended/ar", function () {
      it("check basic minimal stub response", async function () {
        const response = await request.get("/extended/ar?id=2000.01.01.03.0_5&compression=minimal").set({'x-argokey': 'developer'});
        expect(response.body.length).to.eql(1); 
      });
    }); 

    describe("GET /extended/ar?_id", function () {
      it("check basic extended data filtering", async function () {
        const response = await request.get("/extended/ar?id=2000.01.01.03.0_5&data=ivt").set({'x-argokey': 'developer'});
        expect(response.body[0]['data'][0][0]).to.eql(253.9530792236328);
      });
    });

    describe("GET /extended/ar?_id", function () {
      it("make sure except-data-values doesnt melt down", async function () {
        const response = await request.get("/extended/ar?id=2000.01.01.03.0_5&data=except-data-values").set({'x-argokey': 'developer'});
        expect(response.body.length).to.eql(1)
      });
    });
  }
})
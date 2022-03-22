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
    describe("GET /profiles", function () {
      it("searches for profiles by date", async function () {
        const response = await request.get("/profiles?startDate=2006-04-01T00:00:00.000Z&endDate=2006-05-01T00:00:00.000Z&compression=basic").set({'x-argokey': 'developer'});
        expect(response.body).to.be.jsonSchema(schema.paths['/profiles'].get.responses['200'].content['application/json'].schema);
      });
    });

    describe("GET /profiles", function () {
      it("searches for profiles by date and geo", async function () {
        const response = await request.get("/profiles?startDate=2006-04-01T00:00:00.000Z&endDate=2006-05-01T00:00:00.000Z&polygon=[[134.23095703125,38.05674222065296],[131.912841796875,34.7506398050501],[136.219482421875,34.56990638085636],[134.23095703125,38.05674222065296]]&compression=basic").set({'x-argokey': 'developer'});
        expect(response.body).to.be.jsonSchema(schema.paths['/profiles'].get.responses['200'].content['application/json'].schema);
      });
    });

    describe("GET /profiles", function () {
      it("gets only pressure measurements", async function () {
        const response = await request.get("/profiles?startDate=2006-04-01T00:00:00.000Z&endDate=2006-05-01T00:00:00.000Z&polygon=[[134.23095703125,38.05674222065296],[131.912841796875,34.7506398050501],[136.219482421875,34.56990638085636],[134.23095703125,38.05674222065296]]&data=pres&compression=basic").set({'x-argokey': 'developer'});
  
        expect(response.body[0].data_keys).to.eql(['pres']);
      });
    });

    describe("GET /profiles", function () {
      it("fails on too long a date range", async function () {
        const response = await request.get("/profiles?startDate=2020-01-01T00:00:00.000Z&endDate=2020-04-01T00:00:00.000Z&compression=basic").set({'x-argokey': 'developer'});
        expect(response.status).to.eql(400);
      });
    });

    describe("GET /profiles", function () {
      it("gets profiles for one platform", async function () {
        const response = await request.get("/profiles?platforms=2900448&compression=basic").set({'x-argokey': 'developer'});
        expect(response.body).to.be.jsonSchema(schema.paths['/profiles'].get.responses['200'].content['application/json'].schema);
      });
    });

    describe("GET /profiles", function () {
      it("fails on too many platforms", async function () {
        const response = await request.get("/profiles?platforms=4902911,9999&compression=basic").set({'x-argokey': 'developer'});
        expect(response.status).to.eql(400);
      });
    });

    describe("GET /profiles", function () {
      it("fails with an unclosed polygon", async function () {
        const response = await request.get("/profiles?startDate=2020-01-01T00:00:00.000Z&endDate=2020-03-30T00:00:00.000Z&polygon=[[-54.228515625,41.50857729743935],[-57.919921875,36.1733569352216],[-51.328125,36.10237644873644],[-54.228515625,41.50857729743936]]&compression=basic").set({'x-argokey': 'developer'});
        expect(response.status).to.eql(400);
      });
    });

    describe("GET /profiles", function () {
      it("cuts off pressures greater than 100", async function () {
        const response = await request.get("/profiles?startDate=2006-04-01T00:00:00.000Z&endDate=2006-05-01T00:00:00.000Z&polygon=[[134.23095703125,38.05674222065296],[131.912841796875,34.7506398050501],[136.219482421875,34.56990638085636],[134.23095703125,38.05674222065296]]&data=pres&presRange=0,100&compression=basic").set({'x-argokey': 'developer'});
        expect(response.body[0].data[response.body[0].data.length-1][0]).to.be.lessThan(100)
      });
    });

    describe("GET /profiles", function () {
      it("should find 2 profiles within a 20 km of this point", async function () {
        const response = await request.get("/profiles?platforms=2900448&center=134.25,36.16&radius=20&compression=basic").set({'x-argokey': 'developer'});
        expect(response.body.length).to.eql(2)
      });
    });

    describe("GET /profiles", function () {
      it("should only return KO profiles", async function () {
        const response = await request.get("/profiles?ids=2900448_060,2900448_061&dac=KO&compression=basic").set({'x-argokey': 'developer'});
        dacs = response.body.map(p => p.data_center)
        s = new Set(dacs)
        expect(Array.from(s)).to.eql(['KO'])
      });
    });

    describe("GET /profiles", function () {
      it("fails to find any nitrate in BGC measurements", async function () {
        const response = await request.get("/profiles?startDate=2006-04-01T00:00:00.000Z&endDate=2006-05-01T00:00:00.000Z&data=nitrate&compression=basic").set({'x-argokey': 'developer'});
        expect(response.status).to.eql(404);
      });
    });   

    describe("GET /profiles", function () {
      it("finds a core temperature measurement on the given day", async function () {
        const response = await request.get("/profiles?startDate=2006-04-15T00:00:00Z&endDate=2006-04-16T00:00:00Z&data=temp&compression=basic").set({'x-argokey': 'developer'});
        expect(response.status).to.eql(200);
      });
    });

    describe("GET /profiles/overview", function () {
      it("summarizes profile collection", async function () {
        const response = await request.get("/profiles/overview").set({'x-argokey': 'developer'});
        expect(response.body).to.be.jsonSchema(schema.paths['/profiles/overview'].get.responses['200'].content['application/json'].schema);   
      });
    }); 

    describe("GET /profiles/listID", function () {
      it("lists the IDs of any profile with doxy data", async function () {
        const response = await request.get("/profiles/listID?startDate=2006-04-01T00:00:00.000Z&endDate=2006-05-01T00:00:00.000Z&data=doxy").set({'x-argokey': 'developer'});
        expect(response.body).to.have.members(['2900448_060','2900448_061'])  
      });
    }); 

    describe("GET /profiles/listID", function () {
      it("lists the IDs of any profile with a core salinity measurement", async function () {
        const response = await request.get("/profiles/listID?startDate=2006-04-01T00:00:00.000Z&endDate=2006-05-01T00:00:00.000Z&data=psal").set({'x-argokey': 'developer'});
        expect(response.body).to.have.members(['2900448_060','2900448_061'])
      });
    }); 

    describe("GET /profiles/listID", function () {
      it("fails to find any nitrate in BGC measurements", async function () {
        const response = await request.get("/profiles/listID?startDate=2006-04-01T00:00:00.000Z&endDate=2006-05-01T00:00:00.000Z&data=nitrate").set({'x-argokey': 'developer'});
        expect(response.status).to.eql(404);
      });
    });   

    describe("GET /profiles/listID", function () {
      it("fails to find a profile with core salinity measurement on the given day", async function () {
        const response = await request.get("/profiles/listID?startDate=2006-04-16T00:00:00Z&endDate=2006-04-17T00:00:00Z&data=psal").set({'x-argokey': 'developer'});
        expect(response.status).to.eql(404);
      });
    });

    describe("GET /profiles", function () {
      it("should only return bgc profiles", async function () {
        const response = await request.get("/profiles?ids=2900448_060,2900448_061,2900448_062,3900321_050&source=argo_bgc").set({'x-argokey': 'developer'});
        expect(response.body.length).to.eql(3)
      });
    });
  }
})
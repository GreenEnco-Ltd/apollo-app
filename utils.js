const { default: axios } = require("axios");
const { MongoClient } = require("mongodb");
const DateArray = require("./Data/dates.json");
const { IDInverterList, IDPyronoMeterList, IDGenerationMeterList } = require("./Data/project");

module.exports.connectToDatabase = async () => {
  // const client = new MongoClient(`mongodb://localhost:27017`);
  const client = new MongoClient(`mongodb://GreenEncoDB:GreenEnco2024@greenencodb.cluster-cf6w22y6ekly.eu-west-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false`);

  try {
    await client.connect();
    console.log("Connected to database:");

    return client;
  } catch (error) {
    console.error("Database connection error:", error.message);
    // throw error;
  }
};

module.exports.transformGenMeterData = (data) => {
  if (!data) return null;
  let finalData = data?.reduce((acc, curr) => {
    let date = curr?.ReadingDateTime?.substr(0, 10);
    if (acc[date]) {
      acc[date]["sum"] += curr?.Value;
      acc[date]["count"] += 1;
    } else {
      acc[date] = {
        sum: curr?.Value,
        count: 1,
      };
    }
    return acc;
  }, {});
  let result = [];
  for (const key in finalData) {
    const { sum, count } = finalData[key];
    if (!isNaN(sum) && !isNaN(count)) {
      result.push({ DateTimeStamp: key, netEnergy: sum, TotalDays: count });
    }
  }
  return result;
};

module.exports.transformPyronoMeterData = (data) => {
  if (!data) return null;
  let finalData = data?.reduce((acc, curr) => {
    let date = curr?.ReadingDateTime?.substr(0, 10);
    if (acc[date]) {
      acc[date]["irr1"] += curr?.IrradianceCh1;
      acc[date]["irr2"] += curr?.IrradianceCh2;
      acc[date]["count"] += 1;
    } else {
      acc[date] = {
        irr1: curr?.IrradianceCh1,
        irr2: curr?.IrradianceCh2,
        count: 1,
      };
    }
    return acc;
  }, {});
  let result = [];
  for (const key in finalData) {
    const { irr1, irr2, count } = finalData[key];
    if (!isNaN(irr1) && !isNaN(irr2) && !isNaN(count)) {
      result.push({
        DateTimeStamp: key,
        Irradiance1: irr1,
        Irradiance2: irr2,
        TotalDays: count,
      });
    }
  }
  return result;
};

module.exports.transformInverterData = (data, inverterDetail) => {
  // return
  if (!data) return null;
  let finalData = data?.reduce((acc, curr) => {
    let date = curr?.Datetime?.substr(0, 10);

    if (acc[date]) {
      acc[date]["acCurrent1"] += curr?.["AC Current 1"];
      acc[date]["acCurrent2"] += curr?.["AC Current 2"];
      acc[date]["acCurrent3"] += curr?.["AC Current 3"];
      acc[date]["count"] += 1;
    } else {
      acc[date] = {
        acCurrent1: curr?.["AC Current 1"],
        acCurrent2: curr?.["AC Current 2"],
        acCurrent3: curr?.["AC Current 3"],
        count: 1,
        source: curr?.["source"],
      };
    }

    return acc;
  }, {});
  let result = [];
  for (const key in finalData) {
    const { acCurrent1, source, acCurrent2, acCurrent3, count } =
      finalData[key];
    if (!isNaN(acCurrent1) && !isNaN(acCurrent2) && !isNaN(acCurrent3)) {
      result.push({
        DateTimeStamp: key,
        inverterName: source,
        acCurrent: acCurrent1 + acCurrent2 + acCurrent3,
        acCurrent3,
        acCurrent1,
        acCurrent2,
        acCurrent3,
        TotalDays: count,
      });
    }
  }
  return result;
};

module.exports.fetchAnGenMeter24hData = async ({ year, month, day }) => {
  const client = await this.connectToDatabase();
  const db = client.db("AppoloDatabase3");

  let startDate = year ? year.start : month ? month.start : day.start;
  let endDate = year ? year.end : month ? month.end : day.end;
  let finalArray = DateArray.reduce((acc, curr) => {
    if (
      curr.substring(0, startDate.length) >= startDate &&
      curr.substring(0, endDate.length) <= endDate
    )
      acc.push(curr);
    return acc;
  }, []);
//   console.log("genmeter ", finalArray[0],finalArray[finalArray.length-1])
// return
  for (var i = 0; i < IDGenerationMeterList.length; i++) {
    const deviceId = IDGenerationMeterList[i].id;
    const collectionName = IDGenerationMeterList[i].id?.slice(-5) + "_genMeter" || "";
    const filter1 = JSON.stringify({
      field: "device",
      operator: "=",
      value: deviceId,
    });
    for (var ii = 0; ii < finalArray.length; ii++) {
      const date = finalArray[ii];
      const filter2 = JSON.stringify({
          field: "ReadingDateTime",
          operator: ">=",
          value: `${date}T00:00:00Z`,
        }),
        filter3 = JSON.stringify({
          field: "ReadingDateTime",
          operator: "<=",
          value: `${date}T23:59:59Z`,
        });

      try {
        const response = await axios.get(
          `https://anesco.ardexa.com/api/v1/tables/166961811/an_gen_meter_24h/search`,
          {
            headers: {
              Authorization:
                "Bearer eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoianJveUBncmVlbmVuY28uY28udWsiLCJlbWFpbCI6Impyb3lAZ3JlZW5lbmNvLmNvLnVrIiwic2NvcGVzIjp7IjE2Njk2MTgxMSI6eyJwZXJtaXNzaW9ucyI6WyJyZWFkIl0sImRldkdyb3VwIjoiNWYzZTdiODdlODg0MWFiMDI1MWMxN2EzIn19LCJqdGkiOiIwNDQ3YTAzOC01ZDgwLTRlYzYtOGI3Yi00NzgxM2IzMWMwNDMiLCJpYXQiOjE3MzMyMzU4NDIsImV4cCI6MTc2NDc3MTg0Mn0.GYA19Ijenb458kecJJz9KJKeBZww9ldC-7WzzRGHyG0fIOQyTLtBjKNuNa-N41xpZYcDd_U3LR1bc75zRVX5TQ",
            },
            params: {
              rows: 10000,
              sort: "ReadingDateTime",
              filters: [filter1, filter2, filter3],
            },
          }
        );
        const { records = [] } = await response.data;
        var logDetail = {
          fetchingData: "genMeter",
          projectID: deviceId,
          fetchingDate: date,
        };
        // console.log(records.length)
        if (records?.length > 0) {
          await db.collection(collectionName).insertMany(records);
        }
        await db.collection("genMeter_sucess_logs").insertOne({
          ...logDetail,
          DateTime: new Date().toLocaleString(),
          insertDataLength: records?.length,
          sucess: true,
        });
      } catch (error) {
        await db.collection("genMeter_error_logs").insertOne({
          ...logDetail,
          DateTime: new Date().toLocaleString(),
          error: error?.message || "Error Not Found",
          sucess: false,
        });
      }
    }
  }
};

module.exports.fetchPyranoMeterData = async ({ year, month, day }) => {
  const client = await this.connectToDatabase();
  const db = client.db("AppoloDatabase3");

  let startDate = year ? year.start : month ? month.start : day.start;
  let endDate = year ? year.end : month ? month.end : day.end;
  let finalArray = DateArray.reduce((acc, curr) => {
    if (
      curr.substring(0, startDate.length) >= startDate &&
      curr.substring(0, endDate.length) <= endDate
    )
      acc.push(curr);
    return acc;
  }, []);
//   console.log("pyranometer ", finalArray[0],finalArray[finalArray.length-1])
// return
  for (var i = 0; i < IDPyronoMeterList.length; i++) {
    const deviceId = IDPyronoMeterList[i].id;
    const collectionName = IDPyronoMeterList[i].id?.slice(-5) + "_pyronomter" || "";
    const filter1 = JSON.stringify({
      field: "device",
      operator: "=",
      value: deviceId,
    });
    for (var ii = 0; ii < finalArray.length; ii++) {
      const date = finalArray[ii];
      const filter2 = JSON.stringify({
          field: "ReadingDateTime",
          operator: ">=",
          value: `${date}T00:00:00Z`,
        }),
        filter3 = JSON.stringify({
          field: "ReadingDateTime",
          operator: "<=",
          value: `${date}T23:59:59Z`,
        });
      try {
        const response = await axios.get(
          `https://anesco.ardexa.com/api/v1/tables/166961811/an_pyronometer/search`,
          {
            headers: {
              Authorization:
                "Bearer eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoianJveUBncmVlbmVuY28uY28udWsiLCJlbWFpbCI6Impyb3lAZ3JlZW5lbmNvLmNvLnVrIiwic2NvcGVzIjp7IjE2Njk2MTgxMSI6eyJwZXJtaXNzaW9ucyI6WyJyZWFkIl0sImRldkdyb3VwIjoiNWYzZTdiODdlODg0MWFiMDI1MWMxN2EzIn19LCJqdGkiOiIwNDQ3YTAzOC01ZDgwLTRlYzYtOGI3Yi00NzgxM2IzMWMwNDMiLCJpYXQiOjE3MzMyMzU4NDIsImV4cCI6MTc2NDc3MTg0Mn0.GYA19Ijenb458kecJJz9KJKeBZww9ldC-7WzzRGHyG0fIOQyTLtBjKNuNa-N41xpZYcDd_U3LR1bc75zRVX5TQ",
            },
            params: {
              rows: 10000,
              sort: "ReadingDateTime",
              filters: [filter1, filter2, filter3],
            },
          }
        );
        const { records = [] } = await response.data;
        var logDetail = {
          fetchingData: "pyronometer",
          projectID: deviceId,
          fetchingDate: date,
        };
        // console.log(records.length)
        if (records?.length > 0) {
          await db.collection(collectionName).insertMany(records);
        }
        await db.collection("pyronometer_sucess_logs").insertOne({
          ...logDetail,
          DateTime: new Date().toLocaleString(),
          insertDataLength: records?.length,
          sucess: true,
        });
      } catch (error) {
        await db.collection("pyronometer_error_logs").insertOne({
          ...logDetail,
          DateTime: new Date().toLocaleString(),
          error: error?.message || "Error Not Found",
          sucess: false,
        });
      }
    }
  }
};

module.exports.fetchInverterData = async ({ year, month, day }) => {
  const client = await this.connectToDatabase();
  const db = client.db("AppoloDatabase3");

  let startDate = year ? year.start : month ? month.start : day.start;
  let endDate = year ? year.end : month ? month.end : day.end;
  let finalDate = DateArray.reduce((acc, curr) => {
    if (
      curr.substring(0, startDate.length) >= startDate &&
      curr.substring(0, endDate.length) <= endDate
    )
      acc.push(curr);
    return acc;
  }, []);

//   console.log("inverter ", finalDate[0],finalDate[finalDate.length-1])
// return
  for (let i = 0; i < IDInverterList.length; i++) {
    const inverterList = IDInverterList[i].inverterList || [];
    const deviceId = IDInverterList[i].id;
    const collectionName = IDInverterList[i].id?.slice(-5) + "_inverter" || "";
    const filter1 = JSON.stringify({
      field: "device",
      operator: "=",
      value: deviceId,
    });
    for (var ii = 0; ii < inverterList.length; ii++) {
      var inverter = inverterList[ii];
      const filter2 = JSON.stringify({
        field: "source",
        operator: "=",
        value: inverter,
      });
      for (var iii = 0; iii < finalDate.length; iii++) {
        let date = finalDate[iii];
        const filter3 = JSON.stringify({
            field: "Datetime",
            operator: ">=",
            value: `${date}T00:00:00.000Z`,
          }),
          filter4 = JSON.stringify({
            field: "Datetime",
            operator: "<=",
            value: `${date}T23:59:59.000Z`,
          });

        try {
          const response = await axios.get(
            `https://anesco.ardexa.com/api/v1/tables/166961811/solar/search`,
            {
              headers: {
                Authorization:
                  "Bearer eyJhbGciOiJFUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoianJveUBncmVlbmVuY28uY28udWsiLCJlbWFpbCI6Impyb3lAZ3JlZW5lbmNvLmNvLnVrIiwic2NvcGVzIjp7IjE2Njk2MTgxMSI6eyJwZXJtaXNzaW9ucyI6WyJyZWFkIl0sImRldkdyb3VwIjoiNWYzZTdiODdlODg0MWFiMDI1MWMxN2EzIn19LCJqdGkiOiIwNDQ3YTAzOC01ZDgwLTRlYzYtOGI3Yi00NzgxM2IzMWMwNDMiLCJpYXQiOjE3MzMyMzU4NDIsImV4cCI6MTc2NDc3MTg0Mn0.GYA19Ijenb458kecJJz9KJKeBZww9ldC-7WzzRGHyG0fIOQyTLtBjKNuNa-N41xpZYcDd_U3LR1bc75zRVX5TQ",
              },
              params: {
                rows: 10000,
                sort: "Datetime",
                filters: [filter1, filter2, filter3, filter4],
              },
            }
          );
          const { records = [] } = await response.data;
          var logDetail = {
            fetchingData: "inverter",
            projectID: deviceId,
            inverterName: inverter,
            fetchingDate: date,
          };
          // console.log(records.length)
          if (records?.length > 0) {
            await db.collection(collectionName).insertMany(records);
          }
          await db.collection("inverter_sucess_logs").insertOne({
            ...logDetail,
            DateTime: new Date().toLocaleString(),
            insertDataLength: records?.length,
            sucess: true,
          });
        } catch (error) {
          await db.collection("inverter_error_logs").insertOne({
            ...logDetail,
            DateTime: new Date().toLocaleString(),
            error: error?.message || "Error Not Found",
            sucess: false,
          });
        }
      }
    }
  }
};

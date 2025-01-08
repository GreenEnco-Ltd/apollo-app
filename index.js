const express = require("express");
const application = express();
const bodyparser = require("body-parser");
const cors = require("cors");
const cookieparser = require("cookie-parser");
const {
  fetchInverterData,
  fetchPyranoMeterData,
  fetchAnGenMeter24hData,
} = require("./utils");
const schedule = require("node-schedule");
require("dotenv").config({ path: "./config.env" });

const allowedOrigins = [];

const corsOptions = {
  origin: (origin, callback) => {
    if (allowedOrigins.indexOf(origin) !== -1 || !origin) {
      callback(null, true);
    } else {
      console.log("Blocked Origin is ", origin);
      // callback(new Error("Not allowed by CORS"));
      console.log("Error: ", "Not allowed by CORS");
    }
  },
  credentials: true,
};

application.use(cors(corsOptions));
application.use(bodyparser.urlencoded({ extended: true }));
application.use(express.json());
application.use(cookieparser());

process.on("uncaughtException", (err) => {
  console.log("Server is closing due to uncaughtException occured!");
  console.log("Error :", err.message);
  server.close(() => {
    process.exit(1);
  });
});

const server = application.listen(process.env.PORT || 8002, () => {
  console.log("Server is running at port " + server.address().port);
});

process.on("unhandledRejection", (err) => {
  console.log("Server is closing due to unhandledRejection occured!");
  console.log("Error is:", err.message);
  server.close(() => {
    process.exit(1);
  });
});

// const job3 = schedule.scheduleJob("0 5 0 * * *", function () {
//   console.log(
//     "Job started at ",
//     new Date().toUTCString(),
//     " ",
//     new Date().toLocaleString()
//   );

//   fetchAnGenMeter24hData({ year: { start: "2020-03-01", end: "2025-01-06" } });
//   fetchPyranoMeterData({ year:  { start: "2020-03-01", end: "2025-01-06" } });
//   fetchInverterData({ year: { start: "2019-10-04", end: "2025-01-06" } });
// });

// fetchAnGenMeter24hData({ year: { start: "2020-03-01", end: "2025-01-06" } });
// fetchPyranoMeterData({ year:  { start: "2020-03-01", end: "2025-01-06" } });
fetchInverterData({ year: { start: "2019-01-06", end: "2025-01-06" } });
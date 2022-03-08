const mongoose = require("mongoose");

require("dotenv").config();

const getConnection = async () => {
  if (mongoose.connection.readyState == 1) {
    return true;
  } else {
    return false;
  }
};

const initConnection = async () => {
  try {
    await mongoose.connect(
      `mongodb://${process.env.USER}:${process.env.PASSWORD}@${process.env.SERVER_ADDRESS}:${process.env.PORT}/?authSource=admin&readPreference=primary&directConnection=true&ssl=false`,
      {
        useNewUrlParser: true,
        useUnifiedTopology: true,
      }
    );
    console.log("CONNECTED...");
  } catch (error) {
    console.log("CONNECTION TO DATABASE FAILED.");
    process.exit(1);
  }
};

module.exports = { getConnection, initConnection };

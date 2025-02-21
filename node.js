const express = require("express");
const { Pool } = require("pg");
const fs = require("fs");
const path = require("path");
const csv = require("csv-parser");
require("dotenv").config();

const app = express();
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Function to create the `users` table if it doesn't exist
const createUsersTable = async () => {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS public.users (
      id SERIAL PRIMARY KEY,
      name VARCHAR NOT NULL,
      age INT NOT NULL,
      address JSONB,
      additional_info JSONB
    );
  `;
  await pool.query(createTableQuery);
  console.log("Table created or already exists.");
};

// Function to load CSV data into a temp table and then process it
const loadCsvToTempTable = async (csvFilePath) => {
  const tempTableColumns = [];
  const additionalInfoColumns = [];

  // Read the CSV file to get the headers
  await new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on("headers", (headers) => {
        headers.forEach((header) => {
          if (header.startsWith("address.")) {
            tempTableColumns.push(`"${header}" VARCHAR`);
          } else if (
            header !== "name.firstName" &&
            header !== "name.lastName" &&
            header !== "age"
          ) {
            additionalInfoColumns.push(header);
            tempTableColumns.push(`"${header}" VARCHAR`);
          } else {
            tempTableColumns.push(`"${header}" VARCHAR`);
          }
        });
        resolve();
      })
      .on("error", reject);
  });

  const createTempTableQuery = `
    CREATE TEMP TABLE temp_users (
      ${tempTableColumns.join(", ")}
    );
  `;

  const copyCsvQuery = `
    COPY temp_users (${tempTableColumns
      .map((col) => col.split(" ")[0])
      .join(", ")})
    FROM '${csvFilePath}'
    WITH (FORMAT csv, HEADER true);
  `;

  const insertIntoUsersTableQuery = `
    INSERT INTO public.users (name, age, address, additional_info)
    SELECT 
      CONCAT(temp."name.firstName", ' ', temp."name.lastName") AS name, 
      temp.age::INT, 
      jsonb_build_object(
        'addressLine1', temp."address.addressLine1",
        'addressLine2', temp."address.addressLine2",
        'city', temp."address.city",
        'state', temp."address.state"
      ) AS address,
      jsonb_build_object(${additionalInfoColumns
        .map((col) => `'${col}', temp."${col}"`)
        .join(", ")}) AS additional_info
    FROM temp_users AS temp;
  `;

  try {
    await pool.query(createTempTableQuery);
    console.log("Temporary table created.");

    await pool.query(copyCsvQuery);
    console.log("CSV data loaded into temp table.");

    await pool.query(insertIntoUsersTableQuery);
    console.log("Data inserted into users table.");
  } catch (error) {
    console.error("Error processing CSV:", error);
  }
};

const calculateAgeDistribution = async () => {
  try {
    // Query to calculate user count per age group
    const ageDistributionQuery = `
        SELECT 
          SUM(CASE WHEN age < 20 THEN 1 ELSE 0 END) AS "under_20",
          SUM(CASE WHEN age >= 20 AND age <= 40 THEN 1 ELSE 0 END) AS "20_to_40",
          SUM(CASE WHEN age > 40 AND age <= 60 THEN 1 ELSE 0 END) AS "40_to_60",
          SUM(CASE WHEN age > 60 THEN 1 ELSE 0 END) AS "over_60",
          COUNT(*) AS total_users
        FROM public.users;
      `;

    const result = await pool.query(ageDistributionQuery);

    const {
      under_20,
      "20_to_40": twenty_to_forty,
      "40_to_60": forty_to_sixty,
      over_60,
      total_users,
    } = result.rows[0];

    // Calculate the percentage distribution for each age group
    const under20Percent = ((under_20 / total_users) * 100).toFixed(2);
    const twentyToFortyPercent = (
      (twenty_to_forty / total_users) *
      100
    ).toFixed(2);
    const fortyToSixtyPercent = ((forty_to_sixty / total_users) * 100).toFixed(
      2
    );
    const over60Percent = ((over_60 / total_users) * 100).toFixed(2);

    // Print the report
    console.log("Age-Group % Distribution");
    console.log(`< 20: ${under20Percent}%`);
    console.log(`20 to 40: ${twentyToFortyPercent}%`);
    console.log(`40 to 60: ${fortyToSixtyPercent}%`);
    console.log(`> 60: ${over60Percent}%`);
  } catch (error) {
    console.error("Error calculating age distribution:", error);
  }
};

// Endpoint to trigger the CSV loading process
app.get("/upload", async (req, res) => {
  try {
    await createUsersTable();
    const csvFilePath = path.resolve(__dirname, process.env.CSV_FILE_PATH);
    await loadCsvToTempTable(csvFilePath);
    await calculateAgeDistribution();
    res.send("CSV data processed and inserted into users table.");
  } catch (error) {
    console.error("Error:", error);
    res.status(500).send("An error occurred while processing the CSV.");
  }
});

// Start the server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});

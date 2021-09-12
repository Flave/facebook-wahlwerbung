import DBClient from "./DBClient.js";
import { readFileSync } from "fs";
import { csvParse } from "d3-dsv";

/* const adsColumns = [
  { name: "id", type: "text" },
  { name: "advertiser", type: "text" },
  { name: "party", type: "text" },
  { name: "platform", type: "text" },
  { name: "date_start", type: "date" },
  { name: "date_end", type: "date" },
  { name: "type", type: "text" },
  { name: "spend_min_eur", type: "int" },
  { name: "spend_max_eur", type: "int" },
  { name: "age_targeting", type: "text" },
  { name: "gender_targeting", type: "text" },
  { name: "impressions", type: "text" },
  { name: "geo_targeting_included", type: "text[]" },
  { name: "geo_targeting_excluded", type: "text[]" },
  { name: "audience", type: "json" },
  { name: "impressions_min", type: "int" },
  { name: "impressions_max", type: "int" },
  { name: "platforms", type: "text[]" },
]; */

const advertisersColumns = [
  { name: "id", type: "text" },
  { name: "platform", type: "text" },
  { name: "name", type: "text" },
  { name: "party", type: "text" },
  { name: "spend_eur", type: "int" },
  /* { name: "audience", type: "json" }, */
];

const partiesColumns = [
  { name: "id", type: "text" },
  { name: "platform", type: "text" },
  { name: "name", type: "text" },
  { name: "main_advertiser", type: "text" },
  { name: "color", type: "varchar(7)" },
  /* { name: "audience", type: "json" }, */
];

const daysColumns = [
  { name: "date", type: "date" },
  { name: "spend_eur", type: "int" },
  { name: "ads_count", type: "int" },
  { name: "advertiser", type: "text" },
  { name: "party", type: "text" },
  { name: "region", type: "text" },
];

const specs = [
  /* {
    fileNames: ["ads"],
    table: "ads",
    columns: adsColumns,
  }, */
  {
    fileNames: ["advertisers"],
    table: "advertisers",
    columns: advertisersColumns,
  },
  {
    fileNames: ["parties"],
    table: "parties",
    columns: partiesColumns,
    tableOptions: { uniqId: false },
  },
  {
    fileNames: ["days"],
    table: "advertisers_by_day",
    columns: daysColumns,
  },
];

const upload = async () => {
  const client = DBClient();

  for (const spec of specs) {
    console.log(`Uploading ${spec.table}`);

    await client.createTable(spec.table, {
      columns: spec.columns,
      ...(spec.tableOptions || {}),
    });

    for (const fileName of spec.fileNames) {
      try {
        const data = csvParse(
          readFileSync(
            new URL(`./data/${fileName}.csv`, import.meta.url),
            "utf-8"
          ),
          (row, index, columns) =>
            columns.reduce((acc, col) => {
              acc[col] = row[col];
              return acc;
            }, {})
        );

        await client.bulkInsertData({
          columns: spec.columns,
          table: spec.table,
          entries: data,
        });
      } catch (e) {
        console.error(e);
      }
    }
  }
};

upload();

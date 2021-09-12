import { readdirSync, readFileSync, writeFileSync } from "fs";
import { csvParse, csvFormat } from "d3-dsv";
import { reduceBy } from "./utils.js";

/*
 * Reads out all files in raw-data/days and creates day by day records for each advertiser and each region
 */

const dataDirectoryIn = new URL("./raw-data/days", import.meta.url);
const advertisersFileIn = new URL(
  "./data/advertisers.csv",
  import.meta.url
);
const daysFileOut = new URL("./data/days.csv", import.meta.url);

const advertisers = csvParse(readFileSync(advertisersFileIn, "utf-8"));
const advertisersById = reduceBy(advertisers);

export const advertiserFields = [
  ["Page ID", "party"],
  ["Amount Spent (EUR)", "spend_eur"],
  ["Number of Ads in Library", "ads_count"],
];

const dayDirectories = readdirSync(dataDirectoryIn).filter(
  (d) => d !== ".DS_Store"
);

let data = [];

const safeParseInt = (val) => {
  const parsed = parseInt(val);
  return isNaN(parsed) ? val : parsed;
};

const getAdvertiserData = (url, staticProps) => {
  const fileText = readFileSync(url, "utf-8");
  const advertisersRaw = csvParse(fileText.substring(1)).filter(
    (advertiser) => advertisersById[advertiser["Page ID"]] // && advertiser['Amount Spent (EUR)'] !== '≤100'
  );

  let records = advertisersRaw.map((advertiserRaw) => {
    const spend = advertiserRaw["Amount Spent (EUR)"];
    const advertiser = advertisersById[advertiserRaw["Page ID"]];
    let record = {
      advertiser: advertiser.id,
      party: advertiser.party,
      spend_eur: spend === "≤100" ? 25 : parseInt(spend),
      ads_count: safeParseInt(advertiserRaw["Number of Ads in Library"]),
      ...staticProps,
    };
    return record;
  });

  // There are duplicate advertisers, some with disclaimer, some without
  records = records.reduce((acc, record) => {
    const aggregate = acc[record.advertiser];
    if (aggregate) {
      acc[record.advertiser] = {
        ...aggregate,
        ads_count:
          record.ads_count !== undefined
            ? aggregate.ads_count + record.ads_count
            : aggregate.ads_count,
        spend_eur: aggregate.spend_eur + record.spend_eur,
      };
    } else {
      acc[record.advertiser] = record;
    }
    return acc;
  }, {});

  return Object.values(records);
};

// Loop through all days and extract advertiser and regional data
dayDirectories.forEach((dir, i) => {
  const date = dir.substring(24, 34);
  const dayDirectory = `./raw-data/days/${dir}`;
  const fileIn = new URL(
    `${dayDirectory}/FacebookAdLibraryReport_${date}_DE_yesterday_advertisers.csv`,
    import.meta.url
  );

  const regionsDirectory = new URL(`${dayDirectory}/regions`, import.meta.url);
  const regionFiles = readdirSync(regionsDirectory).filter(
    (d) => d !== ".DS_Store"
  );
  // Get data for entire germany
  const records = getAdvertiserData(fileIn, { region: "deutschland", date });
  data = [...data, ...records];

  // Loop through all regions
  regionFiles.forEach((regionFile) => {
    const fileIn = new URL(
      `${dayDirectory}/regions/${regionFile}`,
      import.meta.url
    );

    
    const region = regionFile
      .substring(48)
      .replace(".csv", "")
      .toLowerCase()
      .replace(/ü|ü/, "ue"); // Spellings use different types of umlaut characters...
    const records = getAdvertiserData(fileIn, { region, date });
    data = [...data, ...records];
  });
});

writeFileSync(daysFileOut, csvFormat(data));

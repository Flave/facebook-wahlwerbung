import { parties } from "./meta.js";
import { readFileSync, writeFileSync } from "fs";
import { csvParse, csvFormat } from "d3-dsv";


export const advertiserFields = [
  ["Page ID", "id"],
  ["Page Name", "name"],
  ["Amount Spent (EUR)", "spend_eur"],
  ["Disclaimer", "disclaimer"],
  ["Number of Ads in Library", "ads_count"],
];

// This file is taken from the "All dates" ads report under https://www.facebook.com/ads/library/report/
const advertisersFileIn = new URL(
  "./raw-data/FacebookAdLibraryReport_2021-09-10_DE_lifelong_advertisers.csv",
  import.meta.url
);

const advertisersFileOut = new URL(
  "./data/advertisers.csv",
  import.meta.url
);

const partiesFileOut = new URL("./data/parties.csv", import.meta.url);

let advertisersRaw = csvParse(readFileSync(advertisersFileIn, "utf-8"), row => ({
  ...Object.keys(row).reduce((acc, key) => {
    const newKey = key.replace(/^\s/g, '').replace(/\s/g, ' ');
    acc[newKey] = row[key]
    return acc;
  }, {}),
}));

let advertisers = advertisersRaw
  .map((advertiserRaw, i) => {
    let advertiser = advertiserFields.reduce((acc, field) => {
      acc[field[1]] = advertiserRaw[field[0]];
      return acc;
    }, {});

    const party = parties.find(
      ({ tests }) =>
        tests.some((test) => test.test(advertiser.name)) ||
        tests.some((test) => test.test(advertiser.disclaimer))
    );
    advertiser.party = party?.id;
    delete advertiser.disclaimer;
    if (advertiser.spend_eur === "≤100") return;
    advertiser.ads_count = parseInt(advertiser.ads_count);
    advertiser.spend_eur = parseInt(advertiser.spend_eur);
    return advertiser;
  })
  .filter((advertiser) => {
    return advertiser?.party;
  });


// There are duplicate advertisers, some with disclaimer, some without
advertisers = advertisers.reduce((acc, advertiser) => {
  const aggregate = acc[advertiser.id];
  if (aggregate) {
    acc[advertiser.id] = {
      ...aggregate,
      ads_count: aggregate.ads_count + advertiser.ads_count,
      spend_eur: aggregate.spend_eur + advertiser.spend_eur,
    };
  } else {
    acc[advertiser.id] = advertiser;
  }
  return acc;
}, {});

advertisers = Object.values(advertisers);

const extractedParties = parties.map((party) => {
  return {
    id: party.id,
    name: party.name,
    color: party.color,
    main_advertiser: party.main_advertiser,
    advertisers: advertisers
      .filter((advertiser) => advertiser.party === party.id)
      .map((advertiser) => advertiser.id),
  };
});

writeFileSync(advertisersFileOut, csvFormat(advertisers), "utf-8");
writeFileSync(partiesFileOut, csvFormat(extractedParties), "utf-8");

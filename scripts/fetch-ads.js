/*
To run this script a valid fb api access token is required 
 */

import fetch from "node-fetch";
import { readFileSync, createWriteStream } from "fs";
import { Readable } from "stream";
import { csvParse, csvFormatBody, csvFormatRows } from "d3-dsv";

const columns = [
  "id",
  "advertiser",
  "party",
  "date_start",
  "date_stop",
  "spend_min_eur",
  "spend_max_eur",
  "impressions_min",
  "impressions_max",
  "audience",
  "platform",
  "platforms",
];

const adsFileOut = new URL("./data/ads.csv", import.meta.url);
const adsWriteStream = createWriteStream(adsFileOut);
adsWriteStream.on("close", () => console.log("end"));
const readableColumns = Readable.from(`${csvFormatRows([columns])}\n`);
readableColumns.pipe(adsWriteStream, { end: false });

const advertisersFile = new URL(
  "./data/advertisers.csv",
  import.meta.url
);
const advertisers = csvParse(readFileSync(advertisersFile, "utf-8"));

const dateMin = "2021-07-05";
//const today = new Date();

const apiUrl = `
  https://graph.facebook.com/v10.0/ads_archive?
  access_token=${process.env.FB_ACCESS_TOKEN}&
  ad_reached_countries=['DE']&
  ad_type=POLITICAL_AND_ISSUE_ADS&
  ad_delivery_date_min=${dateMin}&
  limit=200&
  fields=spend,demographic_distribution,impressions,ad_delivery_start_time,ad_delivery_stop_time,publisher_platforms,ad_snapshot_url
`;

const getApiUrl = (id) => `${apiUrl}&search_page_ids=[${id}]`;

const fetchAdsForAdvertiser = async (advertiser, i) => {
  console.log(
    `${i.toString().padStart(advertisers.length.toString().length, 0)} - ${
      advertiser.name
    } (${advertiser.party})`
  );
  const url = getApiUrl(advertiser.id);
  let next = url;
  let advertiserAds = [];
  while (next) {
    const adsRaw = await (await fetch(next)).json();
    if (adsRaw.data) {
      const ads = adsRaw.data.map((ad) => {
        return {
          id: ad.id,
          advertiser: advertiser.id,
          party: advertiser.party,
          platform: "facebook",
          date_start: ad.ad_delivery_start_time,
          date_stop: ad.ad_delivery_stop_time,
          spend_min_eur: ad.spend.lower_bound,
          spend_max_eur: ad.spend.upper_bound,
          impressions_min: ad.impressions.lower_bound,
          impressions_max: ad.impressions.upper_bound,
          platforms: ad.publisher_platforms.join(),
          snapshot_url: ad.ad_snapshot_url,
          audience: JSON.stringify(ad.demographic_distribution),
        };
      });
      advertiserAds = [...advertiserAds, ...ads];
    } else {
      console.log(adsRaw);
    }

    next = adsRaw.paging?.next;
  }
  if (advertiserAds.length) {
    const lineBreak = i === advertisers.length - 1 ? "" : "\n";
    const adsReadableStream = Readable.from(
      `${csvFormatBody(advertiserAds, columns)}${lineBreak}`
    );
    adsReadableStream.pipe(adsWriteStream, { end: false });
  }
};

const fetchAds = async () => {
  let i = 0;
  for (const advertiser of advertisers) {
    await fetchAdsForAdvertiser(advertiser, i);
    i += 1;
  }
};

fetchAds();

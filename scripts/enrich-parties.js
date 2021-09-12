/*
  This file puts additional information such as demographic data that exists 
  only on a per advertising base on each advertiser and party
*/

import { readFileSync, writeFileSync } from 'fs';
import { csvParse, csvFormat } from 'd3-dsv';
import { groupBy } from './utils.js';

const adsFileIn = new URL(`./data/ads.csv`, import.meta.url);
const advertisersFileName = new URL(`./data/advertisers.csv`, import.meta.url);
const partiesFileName = new URL(`./data/parties.csv`, import.meta.url);
const ads = csvParse(readFileSync(adsFileIn, 'utf-8'), (row) => ({
  ...row,
  audience: JSON.parse(row.audience).map((d) => ({ ...d, percentage: parseFloat(d.percentage) }))
}));
const advertisers = csvParse(readFileSync(advertisersFileName, 'utf-8'));
const parties = csvParse(readFileSync(partiesFileName, 'utf-8'));

const adsByAdvertiser = Object.values(groupBy(ads, 'advertiser'));
const adsByParty = Object.values(groupBy(ads, 'party'));

const aggregateAudience = (groupedAds, groups, key) => {
  // Sum up all audience values each group
  const groupSums = groupedAds.map((group) => {
    return group.reduce((acc, ad) => {
      ad.audience.forEach((item) => {
        const id = `${item.age}_${item.gender}`;
        if (acc[id]) {
          acc[id] += item.percentage;
        } else acc[id] = item.percentage;
      });
      return acc;
    }, {});
  });

  // Get averages of summed up values
  const aggregated = groupedAds.map((group, i) => {
    const sums = groupSums[i];
    const values = Object.entries(sums).map(([key, percentage]) => {
      const [age, gender] = key.split('_');
      return {
        value: percentage / group.length,
        age,
        gender
      };
    });
    return { values, key: group[0][key] };
  });

  return groups.map((group) => {
    const audience = aggregated.find(({ key }) => key === group.id) || { values: [] };
    return { ...group, audience: JSON.stringify(audience?.values) };
  });
};

const enrichedAdvertisers = aggregateAudience(adsByAdvertiser, advertisers, 'advertiser');
const enrichedParties = aggregateAudience(adsByParty, parties, 'party');

writeFileSync(advertisersFileName, csvFormat(enrichedAdvertisers), 'utf-8');
writeFileSync(partiesFileName, csvFormat(enrichedParties), 'utf-8');

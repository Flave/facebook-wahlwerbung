# Facebook Wahlwerbung

This collection of scripts is made to process and clean up data that is publicly availbale from [Facebook's Public Ads Library on political advertising](https://www.facebook.com/ads/library/report/). There are also other scripts available that process the data that is available via the [API](https://www.facebook.com/ads/library/api). In order to run these, an access token is required which can be set as a `FB_ACCESS_TOKEN` environment variable.

Under `/scripts/raw-data` a file containing all the political advertisers that have been registered by facebook since the beginning of their collection. This file is used by `extract-parties.js` which filter out all the advertisers that aren't of interest and assigns a political party to each advertiser.

Under `/scripts/raw-data/days` a folder for every day can be found which contains data on individual advertisers as well as regions. These files are used by `create-date-data.js` which compiles the whole list into one single file and cleans up unnecessary properties.
# US COVID-19 Reporting ANT\$ Bot

This is the source code that logs data from the [JSU COVID-19 Data Repository](https://github.com/CSSEGISandData/COVID-19) to a [Symbol](nemtech.github.io) blockchain instance. The data is communicated through [IoDLT's](iodlt.com) ANT\$ protocol, where a schema and archive validate and holds this data on-chain.

# Running

This project is written in Typescript. You must have a `.env` file with a `KEY=` variable (this represents the key of the signer).

```
npm install
tsc
npm run start
```

# More about ANT\$

ANTS is a new method of collecting information from a distributed, incentivized array of sources. Users, referred to as the “ants”, are able to be rewarded by contributing to an open data market on the blockchain. Much like a biological system of ants, The ANT\$ system depends on the distribution of ants over a specific area to report and relay authentic information.

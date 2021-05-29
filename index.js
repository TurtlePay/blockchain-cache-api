// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

require('dotenv').config()
const BigInteger = require('big-integer')
const BodyParser = require('body-parser')
const Compression = require('compression')
const CoinUtils = new (require('turtlecoin-utils').CryptoNote)()
const Config = require('./config.json')
const DatabaseBackend = require('./lib/databaseBackend')
const DNS = require('dns')
const Express = require('express')
const Helmet = require('helmet')
const Helpers = require('./lib/helpers')
const isHex = require('is-hex')
const Logger = require('./lib/logger')
const RabbitMQ = require('./lib/rabbit')
const semver = require('semver')
const Transaction = require('turtlecoin-utils').Transaction
const util = require('util')

/* Load in our environment variables */
const env = {
  mysql: {
    host: process.env.MYSQL_HOST || 'localhost',
    port: process.env.MYSQL_PORT || 3306,
    username: process.env.MYSQL_USERNAME || false,
    password: process.env.MYSQL_PASSWORD || false,
    database: process.env.MYSQL_DATABASE || false,
    connectionLimit: process.env.MYSQL_CONNECTION_LIMIT || 10,
    redis: {
      host: process.env.REDIS_HOST || '127.0.0.1',
      port: process.env.REDIS_PORT || 6379,
      defaultTTL: process.env.REDIS_TTL || 15,
      enable: process.env.USE_REDIS || Config.useRedisCache || false
    }
  },
  publicRabbit: {
    host: process.env.RABBIT_PUBLIC_SERVER || 'localhost',
    username: process.env.RABBIT_PUBLIC_USERNAME || '',
    password: process.env.RABBIT_PUBLIC_PASSWORD || ''
  },
  useNodeMonitor: process.env.USE_NODE_MONITOR || Config.useNodeMonitor || false,
  usePoolMonitor: process.env.USE_POOL_MONITOR || Config.usePoolMonitor || false,
  checkPointsDomain: process.env.CHECKPOINTS_DOMAIN || Config.checkPointsDomain || false
}

if (!process.env.NODE_ENV || process.env.NODE_ENV.toLowerCase() !== 'production') {
  Logger.warning('Node.js is not running in production mode. Consider running in production mode: export NODE_ENV=production'.yellow)
}

/* Sanity check to make sure we have connection information
   for the database */
if (!env.mysql.host || !env.mysql.port || !env.mysql.username || !env.mysql.password) {
  Logger.error('It looks like you did not export all of the required connection information into your environment variables before attempting to start the service.')
  process.exit(1)
}

/* Set up our database connection */
const database = new DatabaseBackend({
  host: env.mysql.host,
  port: env.mysql.port,
  username: env.mysql.username,
  password: env.mysql.password,
  database: env.mysql.database,
  connectionLimit: env.mysql.connectionLimit,
  redis: env.mysql.redis,
  usePoolMonitor: env.usePoolMonitor
})

Logger.log('[DB] Connected to database backend at %s:%s', database.host, database.port)

/* Set up our RabbitMQ Helper */
const rabbit = new RabbitMQ(env.publicRabbit.host, env.publicRabbit.username, env.publicRabbit.password)
rabbit.on('log', log => {
  Logger.log('[RABBIT] %s', log)
})

rabbit.on('connect', () => {
  Logger.log('[RABBIT] connected to server at %s', env.publicRabbit.host)
})

const app = Express()

app.use((req, res, next) => {
  const ip = Helpers.requestIp(req)
  if (Config.blacklistedIps.indexOf(ip) !== -1) {
    return res.status(403).send()
  }
  next()
})

/* Automatically decode JSON input from client requests */
app.use(BodyParser.json())

/* Catch body-parser errors */
app.use((err, req, res, next) => {
  if (err instanceof SyntaxError) {
    return res.status(400).send()
  }
  next()
})

/* Set up a few of our headers to make this API more functional */
app.use((req, res, next) => {
  res.header('X-Requested-With', '*')
  res.header('Access-Control-Allow-Origin', Config.corsHeader)
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept, User-Agent')
  res.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
  res.header('Cache-Control', 'max-age=30, public')
  res.header('Referrer-Policy', 'no-referrer')
  res.header('Content-Security-Policy', 'default-src \'none\'')
  res.header('Feature-Policy', 'geolocation none;midi none;notifications none;push none;sync-xhr none;microphone none;camera none;magnetometer none;gyroscope none;speaker self;vibrate none;fullscreen self;payment none;')
  next()
})

/* Set up our system to use Helmet */
app.use(Helmet())

/* If we are configured to use compression in our config, we will activate it */
if (Config.useCompression) {
  app.use(Compression())
}

if (env.checkPointsDomain) {
  /* Gets the current checkpoints IPFS hash */
  app.get('/checkpointsIPFSHash', (req, res) => {
    const start = process.hrtime()

    DNS.resolveTxt(util.format('_dnslink.%s', env.checkPointsDomain), (err, records) => {
      if (err) {
        Helpers.logHTTPError(req, err.toString(), process.hrtime(start))
        return res.status(500).send()
      }

      if (records.length === 0 || records[0].length === 0) {
        Helpers.logHTTPError(req, 'DNS record not found', process.hrtime(start))
        return res.status(404).send()
      }

      const record = records[0][0]

      const hash = record.split('/').pop()

      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json({
        hash: hash
      })
    })
  })
}

/* Return the underlying information about the daemon(s) we are polling */
app.get('/info', (req, res) => {
  const start = process.hrtime()

  database.getInfo()
    .then(info => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      info.isCacheApi = true
      return res.json(info)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

app.get('/getinfo', (req, res) => {
  const start = process.hrtime()

  database.getInfo()
    .then(info => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      info.isCacheApi = true
      return res.json(info)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Get information regarding the current cache height */
app.get('/height', (req, res) => {
  const start = process.hrtime()
  var networkData

  database.getInfo()
    .then(info => {
      networkData = info
      return database.getLastBlockHeader()
    })
    .then(header => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      /* We shave one off the cached network_height as the underlying daemons
         misreport this information. The network_height indicates the block
         that the network is looking for, not the last block it found */
      return res.json({
        height: header.height,
        network_height: networkData.network_height - 1
      })
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Get information regarding the current cache height */
app.get('/getheight', (req, res) => {
  const start = process.hrtime()
  var networkData

  database.getInfo()
    .then(info => {
      networkData = info
      return database.getLastBlockHeader()
    })
    .then(header => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      /* We shave one off the cached network_height as the underlying daemons
       misreport this information. The network_height indicates the block
       that the network is looking for, not the last block it found */
      return res.json({
        height: header.height,
        network_height: networkData.network_height - 1
      })
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Get the current circulating currency amount */
app.get('/supply', (req, res) => {
  const start = process.hrtime()

  database.getLastBlockHeader()
    .then(header => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      const supply = (header.alreadyGeneratedCoins / Math.pow(10, Config.coinDecimals)).toFixed(Config.coinDecimals).toString()
      return res.send(supply)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Returns the latest 2,880 (1 day) block statistics to help
   better understand the state of the network */
app.get('/chain/stats', (req, res) => {
  const start = process.hrtime()

  database.getRecentChainStats()
    .then(blocks => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json(blocks)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Returns the average difficulty and network hashrate over the last 2,880 (1 day) blocks */
app.get('/chain/difficulty', async (req, res) => {
  const start = process.hrtime()

  try {
    const averageDifficulty = await database.get24HourDifficulty()

    Helpers.logHTTPRequest(req, process.hrtime(start))

    return res.json(averageDifficulty)
  } catch (error) {
    Helpers.logHTTPError(req, error, process.hrtime(start))
    return res.status(500).send()
  }
})

/* Submit a new block to the network */
app.post('/block', (req, res) => {
  const start = process.hrtime()
  const blockBlob = req.body.block || false

  if (!blockBlob || !isHex(blockBlob)) {
    const message = 'Invalid block blob format'
    Helpers.logHTTPError(req, message, process.hrtime(start))
    return res.status(400).json({ message: message })
  }

  rabbit.requestReply(Config.queues.relayAgent, {
    blockBlob: blockBlob
  }, 5000)
    .then(response => {
      if (response.error) {
      /* Log and spit back the response */
        Helpers.logHTTPError(req, JSON.stringify(req.body), process.hrtime(start))
        return res.status(400).json({ message: response.error })
      } else {
      /* Log and spit back the response */
        Helpers.logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
        return res.send(201).send()
      }
    })
    .catch(() => {
      Helpers.logHTTPError(req, 'Could not complete request with relay agent', process.hrtime(start))
      return res.status(504).send()
    })
})

/* Get block information for the last 1,000 blocks before
   the specified block inclusive of the specified blocks */
app.get('/block/headers/:search/bulk', (req, res) => {
  const start = process.hrtime()
  const idx = Helpers.toNumber(req.params.search) || -1

  /* If the caller did not specify a valid height then
     they most certainly didn't read the directions */
  if (idx === -1) {
    Helpers.logHTTPError(req, 'No valid height provided', process.hrtime(start))
    return res.status(400).send()
  }

  database.getBlocks(idx, 1000)
    .then(blocks => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json(blocks)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Get block information for the last 30 blocks before
   the specified block inclusive of the specified block */
app.get('/block/headers/:search', (req, res) => {
  const start = process.hrtime()
  const idx = Helpers.toNumber(req.params.search) || -1

  /* If the caller did not specify a valid height then
     they most certainly didn't read the directions */
  if (idx === -1) {
    Helpers.logHTTPError(req, 'No valid height provided', process.hrtime(start))
    return res.status(400).send()
  }

  database.getBlocks(idx)
    .then(blocks => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json(blocks)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Get the last block header */
app.get('/block/header/top', (req, res) => {
  const start = process.hrtime()

  database.getLastBlockHeader()
    .then(header => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json(header)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Get the block header for the specified block (by hash or height) */
app.get('/block/header/:search', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  /* If we suspect that we were passed a hash, let's go look for it */
  if (idx.length === 64) {
    /* But first, did they pass us only hexadecimal characters ? */
    if (!isHex(idx)) {
      Helpers.logHTTPError(req, 'Block hash is not in a valid format', process.hrtime(start))
      return res.status(400).send()
    }

    database.getBlockHeaderByHash(idx)
      .then(header => {
        Helpers.logHTTPRequest(req, process.hrtime(start))
        return res.json(header)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(404).send()
      })
  } else {
    /* If they didn't pass us a number, we need to get out of here */
    if (Helpers.toNumber(idx) === false) {
      Helpers.logHTTPError(req, 'Block height is not a number', process.hrtime(start))
      return res.status(400).send()
    }

    database.getBlockHeaderByHeight(idx)
      .then(header => {
        Helpers.logHTTPRequest(req, process.hrtime(start))
        return res.json(header)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(404).send()
      })
  }
})

/* Get the count of blocks in the backend database */
app.get('/block/count', (req, res) => {
  const start = process.hrtime()

  database.getBlockCount()
    .then(count => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json({
        blockCount: count
      })
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Get a block template for mining */
app.post('/block/template', (req, res) => {
  const start = process.hrtime()
  const address = req.body.address || false
  const reserveSize = Helpers.toNumber(req.body.reserveSize)

  /* If they didn't provide a reserve size then there's little we can do here */
  if (!reserveSize) {
    var error = 'Missing reserveSize value'
    Helpers.logHTTPError(req, error, process.hrtime(start))
    return res.status(400).json({ message: error })
  }

  /* If the reserveSize is out of range, then throw an error */
  if (reserveSize < 0 || reserveSize > 255) {
    error = 'reserveSize out of range'
    Helpers.logHTTPError(req, error, process.hrtime(start))
    return res.status(400).json({ message: error })
  }

  /* To get a block template, an address must be supplied */
  if (!address) {
    error = 'Missing address value'
    Helpers.logHTTPError(req, error, process.hrtime(start))
    return res.status(400).json({ message: error })
  }

  try {
    CoinUtils.decodeAddress(address)
  } catch (e) {
    error = 'Invalid address supplied'
    Helpers.logHTTPError(req, error, process.hrtime(start))
    return res.status(400).json({ message: error })
  }

  rabbit.requestReply(Config.queues.relayAgent, {
    walletAddress: address,
    reserveSize: reserveSize
  }, 5000)
    .then(response => {
      if (response.error) {
        /* Log and spit back the response */
        Helpers.logHTTPError(req, JSON.stringify(req.body), process.hrtime(start))
        return res.status(400).json({ message: response.error })
      } else {
        /* Log and spit back the response */
        Helpers.logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
        return res.json({
          blocktemplate: response.blocktemplate_blob,
          difficulty: response.difficulty,
          height: response.height,
          reservedOffset: response.reserved_offset
        })
      }
    })
    .catch(() => {
      Helpers.logHTTPError(req, 'Could not complete request with relay agent', process.hrtime(start))
      return res.status(504).send()
    })
})

/* Get block information for the specified block (by hash or height) */
app.get('/block/:search', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  /* If we suspect that we were passed a hash, let's go look for it */
  if (idx.length === 64) {
    /* But first, did they pass us only hexadecimal characters ? */
    if (!isHex(idx)) {
      Helpers.logHTTPError(req, 'Block hash supplied is not in a valid format', process.hrtime(start))
      return res.status(400).send()
    }

    database.getBlock(idx)
      .then(block => {
        Helpers.logHTTPRequest(req, process.hrtime(start))
        return res.json(block)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(404).send()
      })
  } else {
    /* If they didn't pass us a number, we need to get out of here */
    if (Helpers.toNumber(idx) === false) {
      Helpers.logHTTPError(req, 'Block height supplied is not a valid number', process.hrtime(start))
      return res.status(400).send()
    }

    database.getBlockHeaderByHeight(idx)
      .then(header => {
        return database.getBlock(header.hash)
      })
      .then(block => {
        Helpers.logHTTPRequest(req, process.hrtime(start))
        return res.json(block)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(404).send()
      })
  }
})

app.post('/outputs/:amount', (req, res) => {
  const start = process.hrtime()
  const amount = Helpers.toNumber(req.params.amount) || false
  const globalIndexes = req.body.globalIndexes || false

  if (!amount) {
    Helpers.logHTTPError(req, 'Must specify a valid amount', process.hrtime(start))
    return res.status(400).send()
  }

  if (!Array.isArray(globalIndexes)) {
    Helpers.logHTTPError(req, 'Must supply an array of globalIndexes', process.hrtime(start))
    return res.status(400).send()
  }

  globalIndexes.forEach((offset) => {
    if (!Helpers.toNumber(offset)) {
      Helpers.logHTTPError(req, 'Must supply only numeric globalIndexes', process.hrtime(start))
      return res.status(400).send()
    }
  })

  database.getAmountKeys(amount, globalIndexes)
    .then(response => {
      Helpers.logHTTPRequest(req, JSON.stringify({ amount: amount, globalIndexes: globalIndexes }), process.hrtime(start))
      return res.json(response)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

app.get('/keyImage/:search', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  if (!isHex(idx) || idx.length !== 64) {
    Helpers.logHTTPError(req, 'Key Image supplied is not in a valid format', process.hrtime(start))
    return res.status(400).send()
  }

  database.getKeyImageUse(idx)
    .then(image => {
      Helpers.logHTTPRequest(req, process.hrtime(start))

      if (image.length === 1) {
        return res.json(image[0])
      }

      return res.status(404).send()
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Get the current transaction pool */
app.get('/transaction/pool', (req, res) => {
  const start = process.hrtime()

  database.getTransactionPool()
    .then(transactions => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json(transactions)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Get a transaction by its hash */
app.get('/transaction/:search', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  /* We need to check to make sure that they sent us 64 hexadecimal characters */
  if (!isHex(idx) || idx.length !== 64) {
    Helpers.logHTTPError(req, 'Transaction hash supplied is not in a valid format', process.hrtime(start))
    return res.status(400).send()
  }

  database.getTransaction(idx)
    .then(transaction => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json(transaction)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(404).send()
    })
})

/* Get transaction inputs by its hash */
app.get('/transaction/:search/inputs', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  /* We need to check to make sure that they sent us 64 hexadecimal characters */
  if (!isHex(idx) || idx.length !== 64) {
    Helpers.logHTTPError(req, 'Transaction hash supplied is not in a valid format', process.hrtime(start))
    return res.status(400).send()
  }

  database.getTransactionInputs(idx)
    .then(inputs => {
      if (inputs.length === 0) {
        Helpers.logHTTPRequest(req, process.hrtime(start))
        return res.status(404).send()
      }

      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json(inputs)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Get transaction outputs by its hash */
app.get('/transaction/:search/outputs', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  /* We need to check to make sure that they sent us 64 hexadecimal characters */
  if (!isHex(idx) || idx.length !== 64) {
    Helpers.logHTTPError(req, 'Transaction hash supplied is not in a valid format', process.hrtime(start))
    return res.status(400).send()
  }

  database.getTransactionOutputs(idx)
    .then(outputs => {
      if (outputs.length === 0) {
        Helpers.logHTTPRequest(req, process.hrtime(start))
        return res.status(404).send()
      }

      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json(outputs)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Get all transactions hashes that have the supplied payment ID */
app.get('/transactions/:search', (req, res) => {
  const start = process.hrtime()
  const idx = req.params.search

  /* We need to check to make sure that they sent us 64 hexadecimal characters */
  if (!isHex(idx) || idx.length !== 64) {
    Helpers.logHTTPError(req, 'Payment ID supplied is not in a valid format', process.hrtime(start))
    return res.status(400).send()
  }

  database.getTransactionHashesByPaymentId(idx)
    .then(hashes => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json(hashes)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

app.get('/amounts', (req, res) => {
  const start = process.hrtime()
  database.getMixableAmounts(Config.defaultMixins)
    .then(amounts => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json(amounts)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(404).send()
    })
})

/* Get random outputs for transaction mixing */
app.post('/randomOutputs', (req, res) => {
  const start = process.hrtime()
  const amounts = req.body.amounts || []
  const mixin = Helpers.toNumber(req.body.mixin) || Config.defaultMixins

  /* If it's not an array then we didn't follow the directions */
  if (!Array.isArray(amounts)) {
    Helpers.logHTTPError(req, JSON.stringify(req.body), process.hrtime(start))
    return res.status(400).send()
  }

  /* Check to make sure that we were passed numbers
     for each value in the array */
  for (var i = 0; i < amounts.length; i++) {
    var amount = Helpers.toNumber(amounts[i])
    if (!amount) {
      Helpers.logHTTPError(req, JSON.stringify(req.body), process.hrtime(start))
      return res.status(400).send()
    }
    amounts[i] = amount
  }

  /* Go and try to get our random outputs for a daemon via rabbit MQ */
  rabbit.requestReply(Config.queues.relayAgent, { randomOutputs: { amounts, mixin } }, 5000)
    .then(response => {
      if (response.status && response.status.toLowerCase() !== 'ok') throw new Error('Could not get outputs')

      Helpers.logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
      return res.json(response.outs)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Allow us to get just the information that a wallet needs to find
   the transactions that belong to the wallet */
app.post('/sync', (req, res) => {
  const start = process.hrtime()
  const lastKnownBlockHashes = req.body.lastKnownBlockHashes || []
  const blockCount = Helpers.toNumber(req.body.blockCount) || 100
  const scanHeight = Helpers.toNumber(req.body.scanHeight)

  /* If it's not an array then we didn't follow the directions */
  if (!Array.isArray(lastKnownBlockHashes) && !scanHeight) {
    Helpers.logHTTPError(req, JSON.stringify(req.body), process.hrtime(start))
    return res.status(400).send()
  }

  if (!scanHeight) {
    var searchHashes = []
    /* We need to loop through these and validate that we were
       given valid data to search through and not data that does
       not make any sense */
    lastKnownBlockHashes.forEach((elem) => {
      /* We need to check to make sure that they sent us 64 hexadecimal characters */
      if (elem.length === 64 && isHex(elem)) {
        searchHashes.push(elem)
      }
    })

    /* If, after sanitizing our input, we don't have any hashes
       to search for, then we're going to stop right here and
       say something about it */
    if (searchHashes.length === 0) {
      Helpers.logHTTPError(req, 'No search hashes supplied', process.hrtime(start))
      return res.status(400).send()
    }

    database.getWalletSyncData(searchHashes, blockCount)
      .then(outputs => {
        req.body.lastKnownBlockHashes = req.body.lastKnownBlockHashes.length
        Helpers.logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
        return res.json(outputs)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(404).send()
      })
  } else {
    database.getWalletSyncDataByHeight(scanHeight, blockCount)
      .then(outputs => {
        Helpers.logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
        return res.json(outputs)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(404).send()
      })
  }
})

/* Allows us to provide a method to send a raw transaction on the network
   endpoint that works with our blockchain relay agent workers */
app.post('/transaction', (req, res) => {
  const start = process.hrtime()
  const transaction = req.body.tx_as_hex || false

  /* If there is no transaction or the data isn't hex... we're done here */
  if (!transaction || !isHex(transaction)) {
    Helpers.logHTTPError(req, 'Invalid or no transaction hex data supplied', process.hrtime(start))
    return res.status(400).send()
  }

  const tx = new Transaction()
  try {
    tx.blob = transaction
  } catch (e) {
    Helpers.logHTTPError(req, 'Could not deserialize transaction', process.hrtime(start))
    return res.status(400).send()
  }

  const txHash = tx.hash
  const txBlob = tx.blob

  rabbit.requestReply(Config.queues.relayAgent, {
    rawTransaction: txBlob,
    hash: txHash
  }, 9000)
    .then(response => {
      /* Log and spit back the response */
      Helpers.logHTTPRequest(req, util.format('[%s] [I:%s] [O:%s] [A:%s] [F:%s] [%s] %s', txHash, tx.inputs.length, tx.outputs.length, tx.amount || 0, tx.fee || 0, (response.status) ? response.status.yellow : 'Error'.red, response.error.red), process.hrtime(start))

      if (response.status) {
        return res.json(response)
      } else {
        return res.status(504).send()
      }
    })
    .catch(() => {
      Helpers.logHTTPError(req, 'Could not complete request with relay agent', process.hrtime(start))
      return res.status(504).send()
    })
})

/* Legacy daemon API calls provided for limited support */

app.get('/fee', (req, res) => {
  const start = process.hrtime()

  Helpers.logHTTPRequest(req, process.hrtime(start))

  return res.json({
    address: Config.nodeFee.address,
    amount: Config.nodeFee.amount,
    status: 'OK'
  })
})

app.get('/feeinfo', (req, res) => {
  const start = process.hrtime()

  Helpers.logHTTPRequest(req, process.hrtime(start))

  return res.json({
    address: Config.nodeFee.address,
    amount: Config.nodeFee.amount,
    status: 'OK'
  })
})

app.post('/getwalletsyncdata/preflight', (req, res) => {
  const start = process.hrtime()
  const startHeight = Helpers.toNumber(req.body.startHeight)
  const startTimestamp = Helpers.toNumber(req.body.startTimestamp)
  const blockHashCheckpoints = req.body.blockHashCheckpoints || []

  blockHashCheckpoints.forEach((checkpoint) => {
    /* If any of the supplied block hashes aren't hexadecimal then we're done */
    if (!isHex(checkpoint)) {
      Helpers.logHTTPError(req, 'Block hash supplied is not in a valid format', process.hrtime(start))
      return res.status(400).send()
    }
  })

  /* We cannot supply both values */
  if (startHeight > 0 && startTimestamp > 0) {
    Helpers.logHTTPError(req, 'Cannot supply both startHeight and startTimestamp', process.hrtime(start))
    return res.status(400).send()
  }

  database.legacyGetWalletSyncDataPreflight(startHeight, startTimestamp, blockHashCheckpoints)
    .then(response => {
      req.body.blockHashCheckpoints = blockHashCheckpoints.length
      Helpers.logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))

      if (response.blockHashes.length !== 0) {
        return res.json({
          height: response.height,
          blockCount: response.blockCount,
          blockHashes: response.blockHashes,
          status: 'OK',
          synced: false
        })
      } else {
        return res.json({
          height: response.height,
          blockCount: response.blockCount,
          blockHashes: [],
          status: 'OK',
          synced: true,
          topBlock: {
            height: response.topBlock.height,
            hash: response.topBlock.hash
          }
        })
      }
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

app.post('/getwalletsyncdata', (req, res) => {
  const start = process.hrtime()
  const startHeight = Helpers.toNumber(req.body.startHeight)
  const startTimestamp = Helpers.toNumber(req.body.startTimestamp)
  const blockHashCheckpoints = req.body.blockHashCheckpoints || []
  const blockCount = Helpers.toNumber(req.body.blockCount) || 100
  const skipCoinbaseTransactions = (req.body.skipCoinbaseTransactions)

  blockHashCheckpoints.forEach((checkpoint) => {
    /* If any of the supplied block hashes aren't hexadecimal then we're done */
    if (!isHex(checkpoint)) {
      Helpers.logHTTPError(req, 'Block hash supplied is not in a valid format', process.hrtime(start))
      return res.status(400).send()
    }
  })

  /* We cannot supply both values */
  if (startHeight > 0 && startTimestamp > 0) {
    Helpers.logHTTPError(req, 'Cannot supply both startHeight and startTimestamp', process.hrtime(start))
    return res.status(400).send()
  }

  database.legacyGetWalletSyncData(startHeight, startTimestamp, blockHashCheckpoints, blockCount, skipCoinbaseTransactions)
    .then(response => {
      req.body.blockHashCheckpoints = blockHashCheckpoints.length
      req.body.from = response.from || 0

      if (response.blocks.length >= 1) {
        req.body.range = {
          start: response.blocks[0].blockHeight,
          end: response.blocks[response.blocks.length - 1].blockHeight
        }
      }

      if (response.blocks.length !== 0) {
        Helpers.logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
        return res.json({
          items: response.blocks,
          status: 'OK',
          synced: false
        })
      } else {
        Helpers.logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
        return res.json({
          items: response.blocks,
          status: 'OK',
          synced: true,
          topBlock: {
            height: response.topBlock.height,
            hash: response.topBlock.hash
          }
        })
      }
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

app.get('/getwalletsyncdata/:height/:count', (req, res) => {
  const start = process.hrtime()
  const startHeight = Helpers.toNumber(req.params.height)
  const blockCount = Helpers.toNumber(req.params.count) || 100

  database.legacyGetWalletSyncDataLite(startHeight, blockCount)
    .then(results => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json({ items: results, status: 'OK' })
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

app.get('/getwalletsyncdata/:height', (req, res) => {
  const start = process.hrtime()
  const startHeight = Helpers.toNumber(req.params.height)
  const blockCount = Helpers.toNumber(req.params.count) || 100

  database.legacyGetWalletSyncDataLite(startHeight, blockCount)
    .then(results => {
      Helpers.logHTTPRequest(req, process.hrtime(start))
      return res.json({ items: results, status: 'OK' })
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

app.post('/get_transactions_status', (req, res) => {
  const start = process.hrtime()
  const transactionHashes = req.body.transactionHashes || []

  transactionHashes.forEach((hash) => {
    if (!isHex(hash)) {
      Helpers.logHTTPError(req, 'Transaction has supplied is not in a valid format', process.hrtime(start))
      return res.status(400).send()
    }
  })

  database.getTransactionsStatus(transactionHashes)
    .then(result => {
      Helpers.logHTTPRequest(req, JSON.stringify(req.body), process.hrtime(start))
      return res.json(result)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Allows us to provide a daemon like /sendrawtransaction
   endpoint that works with our blockchain relay agent workers */
app.post('/sendrawtransaction', (req, res) => {
  const start = process.hrtime()
  const transaction = req.body.tx_as_hex || false

  /* If there is no transaction or the data isn't hex... we're done here */
  if (!transaction || !isHex(transaction)) {
    Helpers.logHTTPError(req, 'Invalid or no transaction hex data supplied', process.hrtime(start))
    return res.status(400).send()
  }

  const tx = new Transaction()
  try {
    tx.blob = transaction
  } catch (e) {
    Helpers.logHTTPError(req, 'Could not deserialize transaction', process.hrtime(start))
    return res.status(400).send()
  }

  const txHash = tx.hash
  const txBlob = tx.blob

  rabbit.requestReply(Config.queues.relayAgent, {
    rawTransaction: txBlob,
    hash: txHash
  }, 9000)
    .then(response => {
      /* Log and spit back the response */
      Helpers.logHTTPRequest(req, util.format('[%s] [I:%s] [O:%s] [A:%s] [F:%s] [%s] %s', txHash, tx.inputs.length, tx.outputs.length, tx.amount || 0, tx.fee || 0, (response.status) ? response.status.yellow : 'Error'.red, response.error.red), process.hrtime(start))

      if (response.status) {
        return res.json(response)
      } else {
        return res.status(504).send()
      }
    })
    .catch(() => {
      Helpers.logHTTPError(req, 'Could not complete request with relay agent', process.hrtime(start))
      return res.status(504).send()
    })
})

/* Returns the last block reward */
app.get('/reward/last', (req, res) => {
  const start = process.hrtime()

  database.getLastBlockHeader()
    .then(header => {
      Helpers.logHTTPRequest(req, process.hrtime(start))

      const reward = (header.baseReward / Math.pow(10, Config.coinDecimals)).toFixed(Config.coinDecimals).toString()

      return res.send(reward)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Returns the next block reward */
app.get('/reward/next', (req, res) => {
  const start = process.hrtime()
  database.getLastBlockHeader()
    .then(header => {
      Helpers.logHTTPRequest(req, process.hrtime(start))

      const reward = BigInteger(Config.maxSupply)
        .subtract(header.alreadyGeneratedCoins)
        .shiftRight(Config.emissionSpeed)
        .toJSNumber()

      const nextReward = (reward / Math.pow(10, Config.coinDecimals)).toFixed(Config.coinDecimals).toString()

      return res.send(nextReward)
    })
    .catch(error => {
      Helpers.logHTTPError(req, error, process.hrtime(start))
      return res.status(500).send()
    })
})

/* Basic status response via GET that responds to basic monitoring requests */
app.get('/status', (req, res) => {
  return res.json({ status: 'ok' })
})

/* These API methods are only available if we have been
   configured as having access to node monitor data in the
   same database */
if (env.useNodeMonitor) {
  app.get('/node/list', (req, res) => {
    const start = process.hrtime()

    database.getNodeStats()
      .then(stats => {
        Helpers.logHTTPRequest(req, process.hrtime(start))

        const response = {
          nodes: []
        }

        stats.forEach((node) => {
          response.nodes.push({
            name: node.name,
            url: node.hostname,
            port: node.port,
            ssl: (node.ssl === 1),
            cache: (node.cache === 1),
            fee: {
              address: node.feeAddress || '',
              amount: node.feeAmount || 0
            },
            availability: node.availability || 0,
            online: node.status || false,
            version: node.version || '',
            timestamp: node.lastCheckTimestamp || 0
          })
        })

        return res.json(response)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(500).send()
      })
  })

  app.get('/node/list/online', (req, res) => {
    const start = process.hrtime()
    const maxFee = Helpers.toNumber(req.query.max_fee) || false
    var minVersion = req.query.min_version || false

    if (minVersion) {
      minVersion = semver.clean(minVersion)
      if (!semver.valid(minVersion)) minVersion = false
    }

    database.getNodeStats()
      .then(stats => {
        Helpers.logHTTPRequest(req, process.hrtime(start))

        const response = {
          nodes: []
        }

        stats.forEach((node) => {
          if (!node.status) return
          if (maxFee && node.feeAmount >= maxFee) return
          if (!node.version) return
          node.version = semver.clean(node.version)
          if (!semver.valid(node.version)) return
          if (minVersion && semver.lt(node.version, minVersion)) return

          response.nodes.push({
            name: node.name,
            url: node.hostname,
            port: node.port,
            ssl: (node.ssl === 1),
            cache: (node.cache === 1),
            fee: {
              address: node.feeAddress,
              amount: node.feeAmount
            },
            availability: node.availability,
            online: node.status,
            version: node.version,
            timestamp: node.lastCheckTimestamp
          })
        })

        return res.json(response)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(500).send()
      })
  })

  app.get('/node/list/available', (req, res) => {
    const start = process.hrtime()
    const maxFee = Helpers.toNumber(req.query.max_fee) || false
    var minVersion = req.query.min_version || false

    if (minVersion) {
      minVersion = semver.clean(minVersion)
      if (!semver.valid(minVersion)) minVersion = false
    }

    database.getNodeStats()
      .then(stats => {
        Helpers.logHTTPRequest(req, process.hrtime(start))

        const response = {
          nodes: []
        }

        stats.forEach((node) => {
          if (node.availability === 0) return
          if (maxFee && node.feeAmount >= maxFee) return
          if (!node.version) return
          node.version = semver.clean(node.version)
          if (!semver.valid(node.version)) return
          if (minVersion && semver.lt(node.version, minVersion)) return

          response.nodes.push({
            name: node.name,
            url: node.hostname,
            port: node.port,
            ssl: (node.ssl === 1),
            cache: (node.cache === 1),
            fee: {
              address: node.feeAddress,
              amount: node.feeAmount
            },
            availability: node.availability,
            online: node.status,
            version: node.version,
            timestamp: node.lastCheckTimestamp
          })
        })

        return res.json(response)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(500).send()
      })
  })

  app.get('/node/stats', (req, res) => {
    const start = process.hrtime()

    database.getNodeStats()
      .then(stats => {
        Helpers.logHTTPRequest(req, process.hrtime(start))

        const response = []

        stats.forEach((node) => {
          if (!node.availability || node.availability === 0) return

          const obj = {
            name: node.name,
            url: node.hostname,
            port: node.port,
            ssl: (node.ssl === 1),
            cache: (node.cache === 1),
            fee: {
              address: node.feeAddress,
              amount: node.feeAmount
            },
            availability: node.availability,
            online: node.status,
            version: node.version,
            timestamp: node.lastCheckTimestamp,
            height: node.height,
            connectionsIn: node.connectionsIn,
            connectionsOut: node.connectionsOut,
            difficulty: node.difficulty,
            hashrate: node.hashrate,
            txPoolSize: node.txPoolSize,
            history: []
          }

          if (Array.isArray(node.history)) {
            node.history.forEach((evt) => {
              obj.history.push({
                timestamp: evt.timestamp,
                online: evt.status
              })
            })
          }

          obj.history.sort((a, b) => (a.timestamp < b.timestamp) ? 1 : -1)

          response.push(obj)
        })

        return res.json(response)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(500).send()
      })
  })
}

/* These API methods are only available if we have been
   configured as having access to pool monitor data in the
   same database */
if (env.usePoolMonitor) {
  app.get('/pool/list', (req, res) => {
    const start = process.hrtime()

    database.getPoolStats()
      .then(stats => {
        Helpers.logHTTPRequest(req, process.hrtime(start))

        const response = {
          pools: []
        }

        stats.forEach((pool) => {
          response.pools.push({
            name: pool.name,
            url: pool.url,
            api: pool.api,
            type: pool.type,
            miningAddress: pool.miningAddress,
            mergedMining: (pool.mergedMining === 1),
            mergedMiningIsParentChain: (pool.mergedMiningIsParentChain === 1),
            fee: pool.fee || 0,
            minPayout: pool.minPayout || 0,
            timestamp: pool.lastCheckTimestamp || 0,
            availability: pool.availability || 0,
            online: pool.status || false
          })
        })

        return res.json(response)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(500).send()
      })
  })

  app.get('/pool/list/online', (req, res) => {
    const start = process.hrtime()

    database.getPoolStats()
      .then(stats => {
        Helpers.logHTTPRequest(req, process.hrtime(start))

        const response = {
          pools: []
        }

        stats.forEach((pool) => {
          if (!pool.status) return

          response.pools.push({
            name: pool.name,
            url: pool.url,
            api: pool.api,
            type: pool.type,
            miningAddress: pool.miningAddress,
            mergedMining: (pool.mergedMining === 1),
            mergedMiningIsParentChain: (pool.mergedMiningIsParentChain === 1),
            fee: pool.fee,
            minPayout: pool.minPayout,
            timestamp: pool.lastCheckTimestamp,
            availability: pool.availability,
            online: pool.status
          })
        })

        return res.json(response)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(500).send()
      })
  })

  app.get('/pool/list/available', (req, res) => {
    const start = process.hrtime()

    database.getPoolStats()
      .then(stats => {
        Helpers.logHTTPRequest(req, process.hrtime(start))

        const response = {
          pools: []
        }

        stats.forEach((pool) => {
          if (!pool.availability || pool.availability === 0) return

          response.pools.push({
            name: pool.name,
            url: pool.url,
            api: pool.api,
            type: pool.type,
            miningAddress: pool.miningAddress,
            mergedMining: (pool.mergedMining === 1),
            mergedMiningIsParentChain: (pool.mergedMiningIsParentChain === 1),
            fee: pool.fee,
            minPayout: pool.minPayout,
            timestamp: pool.lastCheckTimestamp,
            availability: pool.availability,
            online: pool.status
          })
        })

        return res.json(response)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(500).send()
      })
  })

  app.get('/pool/stats', (req, res) => {
    const start = process.hrtime()

    database.getPoolStats()
      .then(stats => {
        Helpers.logHTTPRequest(req, process.hrtime(start))

        const response = []

        stats.forEach((pool) => {
          if (!pool.availability || pool.availability === 0) return

          const obj = {
            name: pool.name,
            url: pool.url,
            api: pool.api,
            type: pool.type,
            miningAddress: pool.miningAddress,
            mergedMining: (pool.mergedMining === 1),
            mergedMiningIsParentChain: (pool.mergedMiningIsParentChain === 1),
            fee: pool.fee,
            minPayout: pool.minPayout,
            timestamp: pool.lastCheckTimestamp,
            availability: pool.availability,
            online: pool.status,
            height: pool.height,
            hashrate: pool.hashrate,
            miners: pool.miners,
            lastBlock: pool.lastBlock,
            donation: pool.donation,
            history: []
          }

          if (Array.isArray(pool.history)) {
            pool.history.forEach((evt) => {
              obj.history.push({
                timestamp: evt.timestamp,
                online: evt.status
              })
            })
          }

          obj.history.sort((a, b) => (a.timestamp < b.timestamp) ? 1 : -1)

          response.push(obj)
        })

        return res.json(response)
      })
      .catch(error => {
        Helpers.logHTTPError(req, error, process.hrtime(start))
        return res.status(500).send()
      })
  })
}

/* Response to options requests for preflights */
app.options('*', (req, res) => {
  return res.status(200).send()
})

/* This is our catch all to return a 404-error */
app.all('*', (req, res) => {
  Helpers.logHTTPError(req, 'Requested URL not Found (404)')
  return res.status(404).send()
})

rabbit.connect()
  .then(() => {
    app.listen(Config.httpPort, Config.bindIp, () => {
      Logger.log('[HTTP] Server started on %s:%s', Config.bindIp, Config.httpPort)
    })
  })
  .catch(error => {
    Logger.error(error.toString())
    process.exit(1)
  })

// Copyright (c) 2018, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

const Config = require('./config.json')
const DatabaseBackend = require('./lib/databaseBackend.js')
const RabbitMQ = require('amqplib')
const UUID = require('uuid/v4')
const util = require('util')
const Compression = require('compression')
const Helmet = require('helmet')
const BodyParser = require('body-parser')
const Express = require('express')
const isHex = require('is-hex')

/* We need to connect to rabbit to connect back to our
   blockchain relay agents to make sure that we can
   handle the sendrawtransaction calls */
const publicRabbitHost = process.env.RABBIT_PUBLIC_SERVER || 'localhost'
const publicRabbitUsername = process.env.RABBIT_PUBLIC_USERNAME || ''
const publicRabbitPassword = process.env.RABBIT_PUBLIC_PASSWORD || ''

/* Helps us to build the RabbitMQ connection string */
function buildConnectionString (host, username, password) {
  log(util.format('Setting up connection to %s@%s...', username, host))
  var result = ['amqp://']

  if (username.length !== 0 && password.length !== 0) {
    result.push(username + ':')
    result.push(password + '@')
  }

  result.push(host)

  return result.join('')
}

var publicChannel
var replyQueue
(async function () {
  /* Set up our access to the necessary RabbitMQ systems */
  var publicRabbit = await RabbitMQ.connect(buildConnectionString(publicRabbitHost, publicRabbitUsername, publicRabbitPassword))
  publicChannel = await publicRabbit.createChannel()

  /* Set up the RabbitMQ queues */
  await publicChannel.assertQueue(Config.queues.relayAgent, {
    durable: true
  })

  /* Create our worker's reply queue */
  replyQueue = await publicChannel.assertQueue('', { exclusive: true, durable: false })
})()

/* Let's set up a standard logger. Sure it looks cheap but it's
   reliable and won't crash */
function log (message) {
  console.log(util.format('%s: %s', (new Date()).toUTCString(), message))
}

function logHTTPRequest (req, params) {
  params = params || ''
  log(util.format('[REQUEST] (%s) %s %s', req.ip, req.path, params))
}

function logHTTPError (req, message) {
  message = message || 'Parsing error'
  log(util.format('[ERROR] (%s) %s: %s', req.ip, req.path, message))
}

/* This is a special magic function to make sure that when
   we parse a number that the whole thing is actually a
   number */
function toNumber (term) {
  if (typeof term === 'number') {
    return term
  }
  if (parseInt(term).toString() === term) {
    return parseInt(term)
  } else {
    return false
  }
}

/* Set up our database connection */
const database = new DatabaseBackend({
  host: Config.mysql.host,
  port: Config.mysql.port,
  username: Config.mysql.username,
  password: Config.mysql.password,
  database: Config.mysql.database,
  connectionLimit: Config.mysql.connectionLimit
})

log('Connected to database backend at ' + database.host + ':' + database.port)

const app = Express()

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
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept')
  res.header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
  res.header('Cache-Control', 'max-age=30, public')
  next()
})

/* Set up our system to use Helmet */
app.use(Helmet())

/* Last but certainly not least, enable compression because we're going to need it */
app.use(Compression())

/* Return the underlying information about the daemon(s) we are polling */
app.get('/info', (req, res) => {
  database.getInfo().then((info) => {
    logHTTPRequest(req)
    return res.json(info)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

/* Get information regarding the current cache height */
app.get('/height', (req, res) => {
  var networkData
  database.getInfo().then((info) => {
    networkData = info
    return database.getLastBlockHeader()
  }).then((header) => {
    logHTTPRequest(req)
    /* We shave one off the cached network_height as the underlying daemons
       misreport this information. The network_height indicates the block
       that the network is looking for, not the last block it found */
    return res.json({
      height: header.height,
      network_height: networkData.network_height - 1
    })
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

/* Get the current circulating currency amount */
app.get('/supply', (req, res) => {
  database.getLastBlockHeader().then((header) => {
    logHTTPRequest(req)
    const supply = (header.alreadyGeneratedCoins / Math.pow(10, Config.coinDecimals)).toFixed(Config.coinDecimals).toString()
    return res.send(supply)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

/* Submit a new block to the network */
app.post('/block', (req, res) => {
  const blockBlob = req.body.block || false

  if (!blockBlob || !isHex(blockBlob)) {
    const message = 'Invalid block blob format'
    logHTTPError(req, message)
    return res.status(400).json({ message: message })
  }

  var cancelTimer

  /* generate a random request ID */
  const requestId = UUID().toString().replace(/-/g, '')

  /* We need to define how we're going to handle responses on our queue */
  publicChannel.consume(replyQueue.queue, (message) => {
    /* If we got a message back and it was meant for this request, we'll handle it now */
    if (message !== null && message.properties.correlationId === requestId) {
      const response = JSON.parse(message.content.toString())

      /* Acknowledge receipt */
      publicChannel.ack(message)

      /* Cancel our cancel timer */
      if (cancelTimer !== null) {
        clearTimeout(cancelTimer)
      }

      if (response.error) {
        /* Log and spit back the response */
        logHTTPError(req, JSON.stringify(req.body))
        return res.status(400).json({ message: response.error })
      } else {
        /* Log and spit back the response */
        logHTTPRequest(req, JSON.stringify(req.body))
        return res.send(201).send()
      }
    } else {
      /* It wasn't for us, don't acknowledge the message */
      publicChannel.nack(message)
    }
  })

  /* Construct a message that our blockchain relay agent understands */
  const payload = {
    blockBlob: blockBlob
  }

  /* Send it across to the blockchain relay agent workers */
  publicChannel.sendToQueue(Config.queues.relayAgent, Buffer.from(JSON.stringify(payload)), {
    correlationId: requestId,
    replyTo: replyQueue.queue,
    expiration: 5000
  })

  /* Set up our cancel timer in case the message doesn't get handled */
  cancelTimer = setTimeout(() => {
    logHTTPError(req, 'Could not complete request with relay agent')
    return res.status(500).send()
  }, 5500)
})

/* Get block information for the last 1,000 blocks before
   the specified block inclusive of the specified blocks */
app.get('/block/headers/:search/bulk', (req, res) => {
  const idx = toNumber(req.params.search) || -1

  /* If the caller did not specify a valid height then
     they most certainly didn't read the directions */
  if (idx === -1) {
    logHTTPError(req)
    return res.status(400).send()
  }

  database.getBlocks(idx, 1000).then((blocks) => {
    logHTTPRequest(req)
    return res.json(blocks)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

/* Get block information for the last 30 blocks before
   the specified block inclusive of the specified block */
app.get('/block/headers/:search', (req, res) => {
  const idx = toNumber(req.params.search) || -1

  /* If the caller did not specify a valid height then
     they most certainly didn't read the directions */
  if (idx === -1) {
    logHTTPError(req)
    return res.status(400).send()
  }

  database.getBlocks(idx).then((blocks) => {
    logHTTPRequest(req)
    return res.json(blocks)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

/* Get the last block header */
app.get('/block/header/top', (req, res) => {
  database.getLastBlockHeader().then((header) => {
    logHTTPRequest(req)
    return res.json(header)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

/* Get the block header for the specified block (by hash or height) */
app.get('/block/header/:search', (req, res) => {
  const idx = req.params.search

  /* If we suspect that we were passed a hash, let's go look for it */
  if (idx.length === 64) {
    /* But first, did they pass us only hexadecimal characters ? */
    if (!isHex(idx)) {
      logHTTPError(req)
      return res.status(400).send()
    }

    database.getBlockHeaderByHash(idx).then((header) => {
      logHTTPRequest(req)
      return res.json(header)
    }).catch((error) => {
      logHTTPError(req, error)
      return res.status(404).send()
    })
  } else {
    /* If they didn't pass us a number, we need to get out of here */
    if (!toNumber(idx)) {
      logHTTPError(req)
      return res.status(400).send()
    }

    database.getBlockHeaderByHeight(idx).then((header) => {
      logHTTPRequest(req)
      return res.json(header)
    }).catch((error) => {
      logHTTPError(req, error)
      return res.status(404).send()
    })
  }
})

/* Get the count of blocks in the backend database */
app.get('/block/count', (req, res) => {
  database.getBlockCount().then((count) => {
    return res.json({
      blockCount: count
    })
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

/* Get a block template for mining */
app.post('/block/template', (req, res) => {
  const address = req.body.address || false
  const reserveSize = toNumber(req.body.reserveSize)

  /* If they didn't provide a reserve size then there's little we can do here */
  if (!reserveSize) {
    var error = 'Missing reserveSize value'
    logHTTPError(req, error)
    return res.status(400).json({ message: error })
  }

  /* If the reserveSize is out of range, then throw an error */
  if (reserveSize < 0 || reserveSize > 255) {
    error = 'reserveSize out of range'
    logHTTPError(req, error)
    return res.status(400).json({ message: error })
  }

  /* To get a block template, an address must be supplied */
  if (!address) {
    error = 'Missing address value'
    logHTTPError(req, error)
    return res.status(400).json({ message: error })
  }

  var cancelTimer

  /* generate a random request ID */
  const requestId = UUID().toString().replace(/-/g, '')

  /* We need to define how we're going to handle responses on our queue */
  publicChannel.consume(replyQueue.queue, (message) => {
    /* If we got a message back and it was meant for this request, we'll handle it now */
    if (message !== null && message.properties.correlationId === requestId) {
      const response = JSON.parse(message.content.toString())

      /* Acknowledge receipt */
      publicChannel.ack(message)

      /* Cancel our cancel timer */
      if (cancelTimer !== null) {
        clearTimeout(cancelTimer)
      }

      if (response.error) {
        /* Log and spit back the response */
        logHTTPError(req, JSON.stringify(req.body))
        return res.status(400).json({ message: response.error })
      } else {
        /* Log and spit back the response */
        logHTTPRequest(req, JSON.stringify(req.body))
        return res.json({
          blocktemplate: response.blocktemplate_blob,
          difficulty: response.difficulty,
          height: response.height,
          reservedOffset: response.reserved_offset
        })
      }
    } else {
      /* It wasn't for us, don't acknowledge the message */
      publicChannel.nack(message)
    }
  })

  /* Construct a message that our blockchain relay agent understands */
  const payload = {
    walletAddress: address,
    reserveSize: reserveSize
  }

  /* Send it across to the blockchain relay agent workers */
  publicChannel.sendToQueue(Config.queues.relayAgent, Buffer.from(JSON.stringify(payload)), {
    correlationId: requestId,
    replyTo: replyQueue.queue,
    expiration: 5000
  })

  /* Set up our cancel timer in case the message doesn't get handled */
  cancelTimer = setTimeout(() => {
    logHTTPError(req, 'Could not complete request with relay agent')
    return res.status(500).send()
  }, 5500)
})

/* Get block information for the specified block (by hash or height) */
app.get('/block/:search', (req, res) => {
  const idx = req.params.search

  /* If we suspect that we were passed a hash, let's go look for it */
  if (idx.length === 64) {
    /* But first, did they pass us only hexadecimal characters ? */
    if (!isHex(idx)) {
      logHTTPError(req)
      return res.status(400).send()
    }

    database.getBlock(idx).then((block) => {
      logHTTPRequest(req)
      return res.json(block)
    }).catch((error) => {
      logHTTPError(req, error)
      return res.status(404).send()
    })
  } else {
    /* If they didn't pass us a number, we need to get out of here */
    if (!toNumber(idx)) {
      logHTTPError(req)
      return res.status(400).send()
    }

    database.getBlockHeaderByHeight(idx).then((header) => {
      return database.getBlock(header.hash)
    }).then((block) => {
      logHTTPRequest(req)
      return res.json(block)
    }).catch((error) => {
      logHTTPError(req, error)
      return res.status(404).send()
    })
  }
})

/* Get the current transaction pool */
app.get('/transaction/pool', (req, res) => {
  database.getTransactionPool().then((transactions) => {
    logHTTPRequest(req)
    return res.json(transactions)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

/* Get a transaction by its hash */
app.get('/transaction/:search', (req, res) => {
  const idx = req.params.search

  /* We need to check to make sure that they sent us 64 hexadecimal characters */
  if (!isHex(idx) || idx.length !== 64) {
    logHTTPError(req)
    return res.status(400).send()
  }

  database.getTransaction(idx).then((transaction) => {
    logHTTPRequest(req)
    return res.json(transaction)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(404).send()
  })
})

/* Get transaction inputs by its hash */
app.get('/transaction/:search/inputs', (req, res) => {
  const idx = req.params.search

  /* We need to check to make sure that they sent us 64 hexadecimal characters */
  if (!isHex(idx) || idx.length !== 64) {
    logHTTPError(req)
    return res.status(400).send()
  }

  database.getTransactionInputs(idx).then((inputs) => {
    if (inputs.length === 0) {
      logHTTPRequest(req)
      return res.status(404).send()
    }
    logHTTPRequest(req)
    return res.json(inputs)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

/* Get transaction outputs by its hash */
app.get('/transaction/:search/outputs', (req, res) => {
  const idx = req.params.search

  /* We need to check to make sure that they sent us 64 hexadecimal characters */
  if (!isHex(idx) || idx.length !== 64) {
    logHTTPError(req)
    return res.status(400).send()
  }

  database.getTransactionOutputs(idx).then((outputs) => {
    if (outputs.length === 0) {
      logHTTPRequest(req)
      return res.status(404).send()
    }
    logHTTPRequest(req)
    return res.json(outputs)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

/* Get all transactions hashes that have the supplied payment ID */
app.get('/transactions/:search', (req, res) => {
  const idx = req.params.search

  /* We need to check to make sure that they sent us 64 hexadecimal characters */
  if (!isHex(idx) || idx.length !== 64) {
    logHTTPError(req)
    return res.status(400).send()
  }

  database.getTransactionHashesByPaymentId(idx).then((hashes) => {
    logHTTPRequest(req)
    return res.json(hashes)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

app.get('/amounts', (req, res) => {
  database.getMixableAmounts(Config.defaultMixins).then((amounts) => {
    logHTTPRequest(req)
    return res.json(amounts)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(404).send()
  })
})

/* Get random outputs for transaction mixing */
app.post('/randomOutputs', (req, res) => {
  const amounts = req.body.amounts || []
  const mixin = toNumber(req.body.mixin) || Config.defaultMixins

  /* If it's not an array then we didn't follow the directions */
  if (!Array.isArray(amounts)) {
    logHTTPError(req, JSON.stringify(req.body))
    return res.status(400).send()
  }

  /* Check to make sure that we were passed numbers
     for each value in the array */
  for (var i = 0; i < amounts.length; i++) {
    var amount = toNumber(amounts[i])
    if (!amount) {
      logHTTPError(req, JSON.stringify(req.body))
      return res.status(400).send()
    }
    amounts[i] = amount
  }

  /* Go and try to get our random outputs */
  database.getRandomOutputsForAmounts(amounts, mixin).then((randomOutputs) => {
    logHTTPRequest(req, JSON.stringify(req.body))
    return res.json(randomOutputs)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

/* Allow us to get just the information that a wallet needs to find
   the transactions that belong to the wallet */
app.post('/sync', (req, res) => {
  const lastKnownBlockHashes = req.body.lastKnownBlockHashes || []
  const blockCount = toNumber(req.body.blockCount) || 100
  const scanHeight = toNumber(req.body.scanHeight)

  /* If it's not an array then we didn't follow the directions */
  if (!Array.isArray(lastKnownBlockHashes) && !scanHeight) {
    logHTTPError(req, JSON.stringify(req.body))
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
      return res.status(400).send()
    }

    database.getWalletSyncData(searchHashes, blockCount).then((outputs) => {
      logHTTPRequest(req, JSON.stringify(req.body))
      return res.json(outputs)
    }).catch((error) => {
      logHTTPError(req, error)
      return res.status(404).send()
    })
  } else {
    database.getWalletSyncDataByHeight(scanHeight, blockCount).then((outputs) => {
      logHTTPRequest(req, JSON.stringify(req.body))
      return res.json(outputs)
    }).catch((error) => {
      logHTTPError(req, error)
      return res.status(404).send()
    })
  }
})

/* Allows us to provide a method to send a raw transaction on the network
   endpoint that works with our blockchain relay agent workers */
app.post('/transaction', (req, res) => {
  const transaction = req.body.tx_as_hex || false
  var cancelTimer

  /* If there is no transaction or the data isn't hex... we're done here */
  if (!transaction || !isHex(transaction)) {
    logHTTPError(req, 'Invalid or no transaction hex data supplied')
    return res.status(400).send()
  }

  /* generate a random request ID */
  const requestId = UUID().toString().replace(/-/g, '')

  /* We need to define how we're going to handle responses on our queue */
  publicChannel.consume(replyQueue.queue, (message) => {
    /* If we got a message back and it was meant for this request, we'll handle it now */
    if (message !== null && message.properties.correlationId === requestId) {
      const response = JSON.parse(message.content.toString())

      /* Acknowledge receipt */
      publicChannel.ack(message)

      /* Cancel our cancel timer */
      if (cancelTimer !== null) {
        clearTimeout(cancelTimer)
      }

      /* Log and spit back the response */
      logHTTPRequest(req, JSON.stringify(req.body))
      return res.json(response)
    } else {
      /* It wasn't for us, don't acknowledge the message */
      publicChannel.nack(message)
    }
  })

  /* Construct a message that our blockchain relay agent understands */
  const tx = {
    rawTransaction: transaction,
    hash: 'webrequest'
  }

  /* Send it across to the blockchain relay agent workers */
  publicChannel.sendToQueue(Config.queues.relayAgent, Buffer.from(JSON.stringify(tx)), {
    correlationId: requestId,
    replyTo: replyQueue.queue,
    expiration: 5000
  })

  /* Set up our cancel timer in case the message doesn't get handled */
  cancelTimer = setTimeout(() => {
    logHTTPError(req, 'Could not complete request with relay agent')
    return res.status(500).send()
  }, 5500)
})

/* Legacy daemon API calls provided for limited support */

app.post('/getwalletsyncdata', (req, res) => {
  const startHeight = toNumber(req.body.startHeight)
  const startTimestamp = toNumber(req.body.startTimestamp)
  const blockHashCheckpoints = req.body.blockHashCheckpoints || []
  const blockCount = toNumber(req.body.blockCount) || 100

  blockHashCheckpoints.forEach((checkpoint) => {
    /* If any of the supplied block hashes aren't hexadecimal then we're done */
    if (!isHex(checkpoint)) {
      return res.status(400).send()
    }
  })

  /* We cannot supply both values */
  if (startHeight > 0 && startTimestamp > 0) {
    return res.status(400).send()
  }

  database.legacyGetWalletSyncData(startHeight, startTimestamp, blockHashCheckpoints, blockCount).then((results) => {
    logHTTPRequest(req, JSON.stringify(req.body))
    return res.json({ items: results, status: 'OK' })
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

app.post('/get_transactions_status', (req, res) => {
  const transactionHashes = req.body.transactionHashes || []

  transactionHashes.forEach((hash) => {
    if (!isHex(hash)) {
      return res.status(400).send()
    }
  })

  database.getTransactionsStatus(transactionHashes).then((result) => {
    logHTTPRequest(req, JSON.stringify(req.body))
    return res.json(result)
  }).catch((error) => {
    logHTTPError(req, error)
    return res.status(500).send()
  })
})

/* Allows us to provide a daemon like /sendrawtransaction
   endpoint that works with our blockchain relay agent workers */
app.post('/sendrawtransaction', (req, res) => {
  const transaction = req.body.tx_as_hex || false
  var cancelTimer

  /* If there is no transaction or the data isn't hex... we're done here */
  if (!transaction || !isHex(transaction)) {
    logHTTPError(req, 'Invalid or no transaction hex data supplied')
    return res.status(400).send()
  }

  /* generate a random request ID */
  const requestId = UUID().toString().replace(/-/g, '')

  /* We need to define how we're going to handle responses on our queue */
  publicChannel.consume(replyQueue.queue, (message) => {
    /* If we got a message back and it was meant for this request, we'll handle it now */
    if (message !== null && message.properties.correlationId === requestId) {
      const response = JSON.parse(message.content.toString())

      /* Acknowledge receipt */
      publicChannel.ack(message)

      /* Cancel our cancel timer */
      if (cancelTimer !== null) {
        clearTimeout(cancelTimer)
      }

      /* Log and spit back the response */
      logHTTPRequest(req, JSON.stringify(req.body))
      return res.json(response)
    } else {
      /* It wasn't for us, don't acknowledge the message */
      publicChannel.nack(message)
    }
  })

  /* Construct a message that our blockchain relay agent understands */
  const tx = {
    rawTransaction: transaction,
    hash: 'webrequest'
  }

  /* Send it across to the blockchain relay agent workers */
  publicChannel.sendToQueue(Config.queues.relayAgent, Buffer.from(JSON.stringify(tx)), {
    correlationId: requestId,
    replyTo: replyQueue.queue,
    expiration: 5000
  })

  /* Set up our cancel timer in case the message doesn't get handled */
  cancelTimer = setTimeout(() => {
    logHTTPError(req, 'Could not complete request with relay agent')
    return res.status(500).send()
  }, 5500)
})

/* Response to options requests for preflights */
app.options('*', (req, res) => {
  return res.status(200).send()
})

/* This is our catch all to return a 404-error */
app.all('*', (req, res) => {
  logHTTPError(req, 'Requested URL not Found (404)')
  return res.status(404).send()
})

app.listen(Config.httpPort, Config.bindIp, () => {
  log('HTTP server started on ' + Config.bindIp + ':' + Config.httpPort)
})

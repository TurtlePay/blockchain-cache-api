// Copyright (c) 2018, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

const MySQL = require('mysql')
const Random = require('random-number-csprng')

const Self = function (opts) {
  opts = opts || {}
  if (!(this instanceof Self)) return new Self(opts)
  this.host = opts.host || '127.0.0.1'
  this.port = opts.port || 3306
  this.username = opts.username || ''
  this.password = opts.password || ''
  this.database = opts.database || ''
  this.socketPath = opts.socketPath || false
  this.connectionLimit = opts.connectionLimit || 10

  this.db = MySQL.createPool({
    connectionLimit: this.connectionLimit,
    host: this.host,
    port: this.port,
    user: this.username,
    password: this.password,
    database: this.database,
    socketPath: this.socketPath
  })
}

Self.prototype.getLastBlockHeader = function () {
  return new Promise((resolve, reject) => {
    this._query([
      'SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE ',
      '`transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` ',
      'FROM `blocks` ORDER BY `height` DESC LIMIT 1'].join(''), []).then((blocks) => {
      if (blocks.length === 0) {
        return reject(new Error('No blocks found in backend storage'))
      }
      /* We're at the top as far as we know, so our depth is 0 */
      blocks[0].depth = 0
      return resolve(blocks[0])
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getBlockHeaderByHash = function (blockHash) {
  return new Promise((resolve, reject) => {
    var topHeight
    this.getLastBlockHeader().then((block) => {
      topHeight = block.height
      return this._query([
        'SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE ',
        '`transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` ',
        'FROM `blocks` WHERE `hash` = ? LIMIT 1'].join(''), [blockHash])
    }).then((blocks) => {
      if (blocks.length === 0) {
        return reject(new Error('Requested block not found'))
      }
      blocks[0].depth = topHeight - blocks[0].height
      return resolve(blocks[0])
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getBlockHeaderByHeight = function (height) {
  return new Promise((resolve, reject) => {
    var topHeight
    this.getLastBlockHeader().then((block) => {
      topHeight = block.height
      return this._query([
        'SELECT `blocks`.*,(SELECT COUNT(*) FROM `transactions` WHERE ',
        '`transactions`.`blockHash` = `blocks`.`hash`) AS `transactionCount` ',
        'FROM `blocks` WHERE `height` = ? LIMIT 1'].join(''), [height])
    }).then((blocks) => {
      if (blocks.length === 0) {
        return reject(new Error('Requested block not found'))
      }
      blocks[0].depth = topHeight - blocks[0].height
      return resolve(blocks[0])
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getBlockHash = function (height) {
  return new Promise((resolve, reject) => {
    this.getBlockHeaderByHeight(height).then((block) => {
      return resolve(block.hash)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getBlockHeight = function (hash) {
  return new Promise((resolve, reject) => {
    this.getBlockHeaderByHash(hash).then((block) => {
      return resolve(block.height)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getBlockCount = function () {
  return new Promise((resolve, reject) => {
    this._query('SELECT COUNT(*) AS `cnt` FROM `blocks`', []).then((results) => {
      if (results.length !== 1) {
        return reject(new Error('Error when requesting total block count from backend database'))
      }
      return resolve(results[0].cnt)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getTransactionPool = function () {
  return new Promise((resolve, reject) => {
    this._query('SELECT * FROM `transaction_pool`', []).then((results) => {
      if (results.length === 0) {
        return resolve([])
      }
      return resolve(results)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getTransactionHashesByPaymentId = function (paymentId) {
  return new Promise((resolve, reject) => {
    this._query([
      'SELECT `txnHash` AS `hash`,`mixin`,`timestamp`,`fee`,`size`, ',
      '`totalOutputsAmount` AS `amount` ',
      'FROM `transactions` ',
      'WHERE `paymentId` = ? ',
      'ORDER BY `timestamp`'].join(''), [ paymentId ]).then((results) => {
      return resolve(results)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getTransaction = function (hash) {
  return new Promise((resolve, reject) => {
    var result = {}

    this._query([
      'SELECT `transactions`.*, CAST(`unlockTime` AS CHAR) AS `unlockTimeString` ',
      'FROM `transactions` WHERE `transactions`.`txnHash` = ?'
    ].join(''), [ hash ]).then((transactions) => {
      if (transactions.length !== 1) {
        return reject(new Error('Transaction not found'))
      }

      var txn = transactions[0]
      result.tx = {
        amount_out: txn.totalOutputsAmount,
        fee: txn.fee,
        hash: txn.txnHash,
        mixin: txn.mixin,
        paymentId: txn.paymentId,
        size: txn.size,
        extra: txn.extra.toString('hex'),
        unlock_time: txn.unlockTimeString,
        nonce: txn.nonce,
        publicKey: txn.publicKey
      }

      return this.getBlockHeaderByHash(txn.blockHash)
    }).then((block) => {
      result.block = {
        cumul_size: block.size,
        difficulty: block.difficulty,
        hash: block.hash,
        height: block.height,
        timestamp: block.timestamp,
        tx_count: block.transactionCount
      }

      return this.getTransactionInputs(result.tx.hash)
    }).then((inputs) => {
      result.tx.inputs = inputs

      return this.getTransactionOutputs(result.tx.hash)
    }).then((outputs) => {
      result.tx.outputs = outputs

      return this.getLastBlockHeader()
    }).then((header) => {
      result.block.depth = header.height - result.block.height
    }).then(() => {
      return resolve(result)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getBlock = function (hash) {
  return new Promise((resolve, reject) => {
    var result
    var topHeight
    this.getLastBlockHeader().then((header) => {
      topHeight = header.height
      return this.getBlockHeaderByHash(hash)
    }).then((block) => {
      result = block
      result.depth = topHeight - block.height
      return this._query([
        'SELECT `totalOutputsAmount` AS `amount_out`, `fee`, `txnHash` AS `hash`, `size` ',
        'FROM `transactions` WHERE `blockHash` = ?'
      ].join(''), [hash])
    }).then((transactions) => {
      result.transactions = transactions
    }).then(() => {
      return resolve(result)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getBlocks = function (height, count) {
  return new Promise((resolve, reject) => {
    /* We return just 30 blocks inclusive of our height */
    const cnt = count || 30
    const min = height - (cnt - 1)
    const max = height
    this._query([
      'SELECT `size`, `difficulty`, `hash`, `height`, `timestamp`, `nonce`, ',
      '(SELECT COUNT(*) FROM `transactions` WHERE `transactions`.`blockHash` = `blocks`.`hash`) AS `tx_count` ',
      'FROM `blocks` WHERE `height` BETWEEN ? AND ? ',
      'ORDER BY `height` DESC'].join(''), [ min, max ])
      .then((blocks) => {
        return resolve(blocks)
      }).catch((error) => {
        return reject(error)
      })
  })
}

Self.prototype.getWalletSyncDataByHeight = function (scanHeight, blockCount) {
  scanHeight = scanHeight || 0
  blockCount = blockCount || 100

  /* We max this out at 100 blocks per call as otherwise we're returning
     a massive amount of data that is just... well... massive */
  if (blockCount > 100) {
    blockCount = 100
  } else if (blockCount < 1) { /* It's kind of pointless to request 0 blocks */
    blockCount = 1
  }

  const results = []
  const transactionsIdx = {}
  const transactions = []
  const blocksIdx = {}

  return new Promise((resolve, reject) => {
    /* Go get the blocks from the scanHeight provided */
    const min = scanHeight
    const max = min + blockCount

    this._query([
      'SELECT `hash` AS `blockHash`, `height`, `timestamp`  ',
      'FROM `blocks` ',
      'WHERE `height` >= ? AND `height` < ? ',
      'ORDER BY `height`'
    ].join(''), [ min, max ]).then((blocks) => {
      const promises = []

      /* We have the blocks, we need to go get the transactions
         for those blocks */
      for (var i = 0; i < blocks.length; i++) {
        const block = blocks[i]
        block.transactions = []
        blocksIdx[block.blockHash] = results.length
        results.push(block)

        promises.push(this._query([
          'SELECT `blockHash`, `txnHash`, `publicKey`, ',
          '`unlockTime`, CAST(`unlockTime` AS CHAR) as `unlockTimeString`, ',
          '`paymentId` FROM `transactions` WHERE `blockHash` = ?'
        ].join(''), [ block.blockHash ]))
      }

      return Promise.all(promises)
    }).then((txnSets) => {
      const promises = []

      /* Loop through the transactions that came back out and toss
         them on the block they belong to */
      for (var i = 0; i < txnSets.length; i++) {
        for (var j = 0; j < txnSets[i].length; j++) {
          var txn = txnSets[i][j]
          const blockIdx = blocksIdx[txn.blockHash]

          /* We need to store this to make it easier to find
             where we need to insert the data later */
          transactionsIdx[txn.txnHash] = { blockIdx: blockIdx, txnIdx: results[blockIdx].transactions.length }
          transactions.push(txn.txnHash)

          /* Append the transaction to the block */
          results[blockIdx].transactions.push({
            hash: txn.txnHash,
            publicKey: txn.publicKey,
            unlockTime: txn.unlockTimeString,
            paymentId: txn.paymentId,
            inputs: [],
            outputs: []
          })

          /* Let's go get the transaction inputs */
          promises.push(this._query([
            'SELECT `txnHash`, `keyImage`, `amount`, `type` ',
            'FROM `transaction_inputs` ',
            'WHERE `txnHash` = ? ORDER BY `amount`'
          ].join(''), [ txn.txnHash ]))
        }
      }

      return Promise.all(promises)
    }).then((inputSets) => {
      const promises = []

      /* Now that we got out transaction inputs back we
         need to push them into the transaction inputs
         in their related blocks */
      for (var i = 0; i < inputSets.length; i++) {
        for (var j = 0; j < inputSets[i].length; j++) {
          const input = inputSets[i][j]
          const transactionIdx = transactionsIdx[input.txnHash]

          results[transactionIdx.blockIdx].transactions[transactionIdx.txnIdx].inputs.push({
            keyImage: (input.keyImage.length !== 0) ? input.keyImage : false,
            amount: input.amount,
            type: input.type.toString(16).padStart(2, '0')
          })
        }
      }

      /* Now we can go get our transaction outputs */
      for (i = 0; i < transactions.length; i++) {
        promises.push(this._query([
          'SELECT `txnHash`, `outputIndex`, `globalIndex`, ',
          '`key`, `amount`, `type` ',
          'FROM `transaction_outputs` ',
          'WHERE `txnHash` = ? ',
          'ORDER BY `outputIndex`'
        ].join(''), [ transactions[i] ]))
      }

      return Promise.all(promises)
    }).then((outputSets) => {
      /* Now that we got out transaction outputs back we
         need to push them into the transaction outputs
         in their related blocks */
      for (var i = 0; i < outputSets.length; i++) {
        for (var j = 0; j < outputSets[i].length; j++) {
          const output = outputSets[i][j]
          const transactionIdx = transactionsIdx[output.txnHash]

          results[transactionIdx.blockIdx].transactions[transactionIdx.txnIdx].outputs.push({
            index: output.outputIndex,
            globalIndex: output.globalIndex,
            key: output.key,
            amount: output.amount,
            type: output.type.toString(16).padStart(2, '0')
          })
        }
      }
    }).then(() => {
      /* That's it, we're done here, spit it back to the caller */
      return resolve(results)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getWalletSyncData = function (knownBlockHashes, blockCount) {
  blockCount = blockCount || 100

  /* We max this out at 100 blocks per call as otherwise we're returning
     a massive amount of data that is just... well... massive */
  if (blockCount > 100) {
    blockCount = 100
  } else if (blockCount < 1) { /* It's kind of pointless to request 0 blocks */
    blockCount = 1
  }

  const results = []
  const transactionsIdx = {}
  const transactions = []
  const blocksIdx = {}

  return new Promise((resolve, reject) => {
    if (!Array.isArray(knownBlockHashes)) return reject(new Error('You must supply an array of block hashes'))
    if (knownBlockHashes.length === 0) return reject(new Error('You must supply at least one known block hash'))

    var criteria = []
    for (var i = 0; i < knownBlockHashes.length; i++) {
      criteria.push('`hash` = ?')
    }
    criteria = criteria.join(' OR ')

    /* Find out the highest block that we know about */
    this._query('SELECT `height` FROM `blocks` WHERE ' + criteria + ' ORDER BY `height` DESC LIMIT 1', knownBlockHashes).then((results) => {
      if (results.length === 0) {
        return reject(new Error('Could not find any blocks matching the the supplied known block hashes'))
      }

      const min = results[0].height
      const max = min + blockCount

      /* Go get the blocks that are within the range */
      return this._query([
        'SELECT `hash` AS `blockHash`, `height`, `timestamp`  ',
        'FROM `blocks` ',
        'WHERE `height` >= ? AND `height` < ? ',
        'ORDER BY `height`'
      ].join(''), [ min, max ])
    }).then((blocks) => {
      const promises = []

      /* We have the blocks, we need to go get the transactions
         for those blocks */
      for (var i = 0; i < blocks.length; i++) {
        const block = blocks[i]
        block.transactions = []
        blocksIdx[block.blockHash] = results.length
        results.push(block)

        promises.push(this._query([
          'SELECT `blockHash`, `txnHash`, `publicKey`, ',
          '`unlockTime`, CAST(`unlockTime` AS CHAR) AS `unlockTimeString`, ',
          '`paymentId` FROM `transactions` WHERE `blockHash` = ?'
        ].join(''), [ block.blockHash ]))
      }

      return Promise.all(promises)
    }).then((txnSets) => {
      const promises = []

      /* Loop through the transactions that came back out and toss
         them on the block they belong to */
      for (var i = 0; i < txnSets.length; i++) {
        for (var j = 0; j < txnSets[i].length; j++) {
          var txn = txnSets[i][j]
          const blockIdx = blocksIdx[txn.blockHash]

          /* We need to store this to make it easier to find
             where we need to insert the data later */
          transactionsIdx[txn.txnHash] = { blockIdx: blockIdx, txnIdx: results[blockIdx].transactions.length }
          transactions.push(txn.txnHash)

          /* Append the transaction to the block */
          results[blockIdx].transactions.push({
            hash: txn.txnHash,
            publicKey: txn.publicKey,
            unlockTime: txn.unlockTime,
            paymentId: txn.paymentId,
            inputs: [],
            outputs: []
          })

          /* Let's go get the transaction inputs */
          promises.push(this._query([
            'SELECT `txnHash`, `keyImage`, `amount`, `type` ',
            'FROM `transaction_inputs` ',
            'WHERE `txnHash` = ? ORDER BY `amount`'
          ].join(''), [ txn.txnHash ]))
        }
      }

      return Promise.all(promises)
    }).then((inputSets) => {
      const promises = []

      /* Now that we got out transaction inputs back we
         need to push them into the transaction inputs
         in their related blocks */
      for (var i = 0; i < inputSets.length; i++) {
        for (var j = 0; j < inputSets[i].length; j++) {
          const input = inputSets[i][j]
          const transactionIdx = transactionsIdx[input.txnHash]

          results[transactionIdx.blockIdx].transactions[transactionIdx.txnIdx].inputs.push({
            keyImage: (input.keyImage.length !== 0) ? input.keyImage : false,
            amount: input.amount,
            type: input.type.toString(16).padStart(2, '0')
          })
        }
      }

      /* Now we can go get our transaction outputs */
      for (i = 0; i < transactions.length; i++) {
        promises.push(this._query([
          'SELECT `txnHash`, `outputIndex`, `globalIndex`, ',
          '`key`, `amount`, `type` ',
          'FROM `transaction_outputs` ',
          'WHERE `txnHash` = ? ',
          'ORDER BY `outputIndex`'
        ].join(''), [ transactions[i] ]))
      }

      return Promise.all(promises)
    }).then((outputSets) => {
      /* Now that we got out transaction outputs back we
         need to push them into the transaction outputs
         in their related blocks */
      for (var i = 0; i < outputSets.length; i++) {
        for (var j = 0; j < outputSets[i].length; j++) {
          const output = outputSets[i][j]
          const transactionIdx = transactionsIdx[output.txnHash]

          results[transactionIdx.blockIdx].transactions[transactionIdx.txnIdx].outputs.push({
            index: output.outputIndex,
            globalIndex: output.globalIndex,
            key: output.key,
            amount: output.amount,
            type: output.type.toString(16).padStart(2, '0')
          })
        }
      }
    }).then(() => {
      /* That's it, we're done here, spit it back to the caller */
      return resolve(results)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.legacyGetWalletSyncData = function (startHeight, startTimestamp, blockHashCheckpoints, blockCount) {
  if (blockCount > 100) {
    blockCount = 100
  }

  const that = this
  return new Promise(async function (resolve, reject) {
    if (!Array.isArray(blockHashCheckpoints)) return reject(new Error('You must supply an blockHashCheckpoints as an array'))

    var topHeight = 0

    /* If we supplied blockHashCheckpoints, let's find the highest one we can */
    if (blockHashCheckpoints.length > 0) {
      var criteria = []
      for (var i = 0; i < blockHashCheckpoints.length; i++) {
        criteria.push('`hash` = ?')
      }
      criteria = criteria.join(' OR ')

      try {
        const heightSearch = await that._query('SELECT `height` FROM `blocks` WHERE ' + criteria + ' ORDER BY `height` DESC LIMIT 1', blockHashCheckpoints)
        topHeight = heightSearch[0].height + 1
      } catch (e) {
        return reject(e)
      }
    }

    /* Did we get a startTimestamp? If so, let's find out the closest height */
    if (startTimestamp > 0) {
      try {
        const timestampSearch = await that._query('SELECT `height` FROM `blocks` WHERE `timestamp` <= ? ORDER BY `height` DESC LIMIT 1', [ startTimestamp ])
        topHeight = timestampSearch[0].height
      } catch (e) {
        return reject(e)
      }
    }

    /* If we got a startHeight and it's higher than anything else we found, then start from there */
    if (startHeight > topHeight) topHeight = startHeight

    /* Let's try to get the blocks that we need the data for */
    var blocks
    try {
      blocks = await that._query('SELECT `hash`, `height`, `timestamp` FROM `blocks` WHERE `height` >= ? ORDER BY `height` ASC LIMIT ?', [topHeight, blockCount])
    } catch (e) {
      return reject(e)
    }

    /* Loop through the blocks and build out the data we need */
    var promises = []
    blocks.forEach((block) => {
      promises.push(that.buildLegacyWalletDataBlock(block))
    })

    /* Let's see what we got back and then kick it back up to the stack */
    Promise.all(promises).then((dataBlocks) => {
      return resolve(dataBlocks)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getTransactionsByBlock = function (blockHash) {
  return new Promise((resolve, reject) => {
    this._query([
      'SELECT `transactions`.*, CAST(`unlockTime` AS CHAR) AS `unlockTimeString` ',
      'FROM `transactions` WHERE `blockHash` = ?'
    ].join(''), [blockHash]).then((rows) => {
      if (rows.length === 0) return resolve([])
      return resolve(rows)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.buildLegacyWalletDataBlock = function (block) {
  return new Promise((resolve, reject) => {
    /* Set up our base object */
    const obj = {
      blockHash: block.hash,
      blockHeight: block.height,
      blockTimestamp: block.timestamp,
      coinbaseTX: {},
      transactions: []
    }

    /* This is a shortcut for finding our way through the promises */
    function getTransactionOffset (hash) {
      for (var i = 0; i < obj.transactions.length; i++) {
        if (obj.transactions[i].hash === hash) return i
      }
      return false
    }

    /* Let's go get our transcations that are in each block */
    this.getTransactionsByBlock(block.hash).then((transactions) => {
      var coinbaseTX

      /* Loop through the transactions and populate the object; however
         if we find the coinbase transaction (which is the one without
         inputs) we're going to store that somewhere else */
      transactions.forEach((transaction) => {
        if (transaction.totalInputsAmount === 0) {
          obj.coinbaseTX.hash = transaction.txnHash
          obj.coinbaseTX.txPublicKey = transaction.publicKey
          obj.coinbaseTX.unlockTime = transaction.unlockTimeString
          obj.coinbaseTX.outputs = []
          coinbaseTX = transaction.txnHash
        } else {
          obj.transactions.push({
            hash: transaction.txnHash,
            inputs: [],
            outputs: [],
            paymentID: transaction.paymentId,
            txPublicKey: transaction.publicKey,
            unlockTime: transaction.unlockTimeString
          })
        }
      })

      /* Go get our coinbase transaction outputs */
      return this.getTransactionOutputs(coinbaseTX)
    }).then((outputs) => {
      /* Loop through the coinbase transaction outputs and populate the object */
      outputs.forEach((output) => {
        obj.coinbaseTX.outputs.push({
          amount: output.amount,
          key: output.key,
          globalIndex: output.globalIndex
        })
      })

      /* Let's loop through the rest of the transactions and go get their inputs */
      var promises = []
      obj.transactions.forEach((txn) => {
        promises.push(this.getTransactionInputs(txn.hash, false))
      })
      return Promise.all(promises)
    }).then((txnInputsResponses) => {
      /* Process all of those inputs */
      txnInputsResponses.forEach((txnInputs) => {
        txnInputs.forEach((input) => {
          var offset = getTransactionOffset(input.txnHash)
          obj.transactions[offset].inputs.push({
            amount: input.amount,
            k_image: input.keyImage
          })
        })
      })

      /* Let's loop through the rest of the transactions and go get their outputs */
      var promises = []
      obj.transactions.forEach((txn) => {
        promises.push(this.getTransactionOutputs(txn.hash, false))
      })
      return Promise.all(promises)
    }).then((txnOutputsResponses) => {
      /* Process all of those outputs */
      txnOutputsResponses.forEach((txnOutputs) => {
        txnOutputs.forEach((output) => {
          var offset = getTransactionOffset(output.txnHash)
          obj.transactions[offset].outputs.push({
            amount: output.amount,
            key: output.key,
            globalIndex: output.globalIndex
          })
        })
      })
    }).then(() => {
      /* We're done here, return back up the stack */
      return resolve(obj)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getTransactionInputs = function (hash, trim) {
  if (typeof trim === 'undefined') trim = true

  return new Promise((resolve, reject) => {
    this._query('SELECT * FROM `transaction_inputs` WHERE `txnHash` = ? ORDER BY `amount`, `keyImage`', [ hash ]).then((results) => {
      if (results.length === 0) {
        return resolve([])
      }

      for (var i = 0; i < results.length; i++) {
        if (trim) {
          delete results[i].txnHash
        }
        /* Convert this from decimal back to hexadecimal and padded to two positions */
        results[i].type = results[i].type.toString(16).padStart(2, '0')
      }

      return resolve(results)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getTransactionOutputs = function (hash, trim) {
  if (typeof trim === 'undefined') trim = true

  return new Promise((resolve, reject) => {
    this._query('SELECT * FROM `transaction_outputs` WHERE `txnHash` = ? ORDER BY `outputIndex`', [ hash ]).then((results) => {
      if (results.length === 0) {
        return resolve([])
      }

      for (var i = 0; i < results.length; i++) {
        if (trim) {
          delete results[i].txnHash
        }
        /* Convert this from decimal back to hexadecimal and padded to two positions */
        results[i].type = results[i].type.toString(16).padStart(2, '0')
      }

      return resolve(results)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getRandomOutputsForAmounts = function (amounts, mixin) {
  const that = this
  return new Promise((resolve, reject) => {
    if (!Array.isArray(amounts)) return reject(new Error('You must supply an array of amounts'))
    mixin = mixin || 0
    mixin += 1

    /* Build the criteria of the SQL call to figure out what range
       of outputs we have to work with. We need to dedupe the request
       to avoid SQL errors. We do this by tracking the individual amount
       of mixins requested for each amount */
    var criteria = []
    var dedupedAmounts = []
    const mixinCounts = {}
    amounts.forEach((amount) => {
      if (dedupedAmounts.indexOf(amount) === -1) {
        criteria.push(' `amount` = ? ')
        dedupedAmounts.push(amount)
        mixinCounts[amount] = mixin
      } else {
        mixinCounts[amount] += mixin
      }
    })
    criteria = criteria.join(' OR ')

    /* Go get the maximum globalIndexe values for each of the
       amounts we want mixins for */
    this._query([
      'SELECT `amount`, `globalIndex` ',
      'FROM `transaction_outputs_index_maximums` ',
      'WHERE ' + criteria
    ].join(''), dedupedAmounts).then(async function (results) {
      /* If we didn't get back as many maximums as the number of
         amounts that we requested, we've got an error */
      if (results.length !== dedupedAmounts.length) {
        throw new Error('No prior outputs exist for one of the supplied amounts')
      }

      /* We're going to build this all into one big query to
         try to speed some of the responses up a little bit */
      var randomCriteria = []
      var randomValues = []

      /* Loop through the maximum values that we found and create
         the new criteria for the query that will go actually get
         the random outputs that we've selected */
      for (var i = 0; i < results.length; i++) {
        const result = results[i]
        const rnds = []

        /* If the returned maximum value is not as big
           as the requested mixin then we need to short
           circuit and kick back an error */
        if (result.globalIndex < mixin) {
          throw new Error('Not enough mixins available to satisfy the request')
        }

        /* Now we need to take into account the count of the mixins that we need */
        const dedupedMixin = mixinCounts[result.amount]

        /* We need to loop until we find enough unique
           random values to satisfy the request */
        while (rnds.length !== dedupedMixin) {
          const rand = await Random(0, result.globalIndex)
          if (rnds.indexOf(rand) === -1) {
            rnds.push(rand)
          }
        }

        /* Loop through the random indexes that we selected and
           build out our T-SQL statement. Yes, we could have done
           this in the loop above but we wanted to put this comment
           here so that others would understand what we're doing */
        rnds.forEach((rand) => {
          randomCriteria.push(' (`amount` = ? AND `globalIndex` = ?) ')
          randomValues.push(result.amount)
          randomValues.push(rand)
        })
      }
      randomCriteria = randomCriteria.join(' OR ')

      /* Go fetch the actual output information from the database using
         the previously created criteria from above */
      return that._query([
        'SELECT `amount`, `globalIndex` AS `global_amount_index`, `key` AS `out_key` ',
        'FROM `transaction_outputs` WHERE ' + randomCriteria + ' ',
        'ORDER BY `amount` ASC'
      ].join(''), randomValues)
    }).then((results) => {
      const response = []

      /* This probably seems a bit goofy. Since we're fetching
         all of the data needed to build the response at once,
         we need to take the flat data from the database
         and form it up really nice into the output as documented
         in the API and used by a few applications */
      var curObject = { amount: -1 }
      results.forEach((result) => {
        if (result.amount !== curObject.amount || curObject.outs.length === mixin) {
          if (curObject.amount !== -1) {
            /* Sort the outputs in each amount set */
            curObject.outs.sort((a, b) =>
              (a.global_amount_index > b.global_amount_index) ? 1
                : ((b.global_amount_index > a.global_amount_index) ? -1 : 0)
            )

            /* Push the object on to our stack in the response */
            response.push(curObject)
          }
          curObject = {
            amount: result.amount,
            outs: []
          }
        }
        curObject.outs.push({
          global_amount_index: result.global_amount_index,
          out_key: result.out_key
        })
      })
      /* Push the last object on to the response stack to make sure
         that we don't accidentally leave it behind */
      response.push(curObject)

      return response
    }).then((results) => {
      return resolve(results)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getTransactionsStatus = function (hashes) {
  return new Promise((resolve, reject) => {
    const result = {
      status: 'OK',
      transactionsInPool: [],
      transactionsInBlock: [],
      transactionsUnknown: []
    }

    var criteria = []
    for (var i = 0; i < hashes.length; i++) {
      criteria.push('`txnHash` = ?')
    }
    criteria = criteria.join(' OR ')

    this._query([
      'SELECT `txnHash` FROM `transaction_pool` ',
      'WHERE ' + criteria
    ].join(''), hashes).then((txns) => {
      txns.forEach((txn) => {
        result.transactionsInPool.push(txn.txnHash)
      })

      return this._query([
        'SELECT `txnHash` FROM `transactions` ',
        'WHERE ' + criteria
      ].join(''), hashes)
    }).then((txns) => {
      txns.forEach((txn) => {
        result.transactionsInBlock.push(txn.txnHash)
      })
    }).then(() => {
      hashes.forEach((txn) => {
        if (result.transactionsInPool.indexOf(txn) === -1 && result.transactionsInBlock.indexOf(txn) === -1) {
          result.transactionsUnknown.push(txn)
        }
      })
    }).then(() => {
      return resolve(result)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getMixableAmounts = function (mixin) {
  mixin = mixin || 3

  return new Promise((resolve, reject) => {
    this._query([
      'SELECT `toim`.`amount`, `toim`.`globalIndex` + 1 AS `outputs`, `t`.`timestamp`, `b`.`height`, `t`.`txnHash`, `b`.`hash` ',
      'FROM `transaction_outputs_index_maximums` AS `toim` ',
      'LEFT JOIN `transaction_outputs` AS `to` ON `to`.`amount` = `toim`.`amount` AND `to`.`globalIndex` = ? ',
      'LEFT JOIN `transactions` AS `t` ON `t`.`txnHash` = `to`.`txnHash` ',
      'LEFT JOIN `blocks` AS `b` ON `b`.`hash` = `t`.`blockHash` ',
      'ORDER BY `toim`.`amount`'
    ].join(''), [mixin]).then((results) => {
      if (results.length === 0) {
        return resolve([])
      }
      return resolve(results)
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype.getInfo = function () {
  return new Promise((resolve, reject) => {
    this._query('SELECT `payload` FROM `information` WHERE `key` = ?', ['getinfo']).then((results) => {
      if (results.length === 0) {
        return reject(new Error('No record found'))
      }
      return resolve(JSON.parse(results[0].payload))
    }).catch((error) => {
      return reject(error)
    })
  })
}

Self.prototype._query = function (query, args) {
  return new Promise((resolve, reject) => {
    this.db.query(query, args, (error, results, fields) => {
      if (error) {
        return reject(error)
      }
      return resolve(results)
    })
  })
}

module.exports = Self

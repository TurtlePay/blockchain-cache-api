// Copyright (c) 2018-2019, TurtlePay Developers
//
// Please see the included LICENSE file for more information.

'use strict'

const MySQL = require('mysql')
const Random = require('random-number-csprng')
const RedisCache = require('./redisCache.js')
const util = require('util')

class DatabaseBackend {
  constructor (opts) {
    opts = opts || {}

    this.host = opts.host || '127.0.0.1'
    this.port = opts.port || 3306
    this.username = opts.username || ''
    this.password = opts.password || ''
    this.database = opts.database || ''
    this.socketPath = opts.socketPath || false
    this.connectionLimit = opts.connectionLimit || 10
    this.usePoolMonitor = opts.usePoolMonitor || false

    this.db = MySQL.createPool({
      connectionLimit: this.connectionLimit,
      host: this.host,
      port: this.port,
      user: this.username,
      password: this.password,
      database: this.database,
      socketPath: this.socketPath
    })

    if (opts.redis && opts.redis.enable) {
      this.cache = new RedisCache(opts.redis)
      this.cache.on('error', err => console.log(err.toString()))
    }
  }

  checkCache (key) {
    return new Promise((resolve, reject) => {
      if (!this.cache) return resolve(false)

      this.cache.get(key)
        .then(data => {
          if (!data) return resolve(false)
          if (Array.isArray(data) && data.length === 0) return resolve(false)
          if (Object.keys(data).length === 0) return resolve(false)
          return resolve(data)
        })
    })
  }

  query (query, args) {
    args = args || []

    return new Promise((resolve, reject) => {
      this.db.query(query, args, (error, results, fields) => {
        if (error) return reject(error)

        return resolve(results)
      })
    })
  }

  setCache (key, value, ttl) {
    return new Promise(resolve => {
      if (!this.cache) return resolve(value)

      this.cache.set(key, value, ttl)
        .then(() => { return resolve(value) })
        .catch(() => { return resolve(value) })
    })
  }

  async get24HourDifficulty () {
    const result = {
      difficulty: 0,
      hashrate: 0
    }

    const cacheName = '24hourdifficulty'

    const cached = await this.checkCache(cacheName)

    if (cached) {
      return cached
    }

    const blocks = await this.query('SELECT `difficulty` FROM `blocks` ORDER BY `height` DESC LIMIT 2880')

    if (blocks.length === 0) {
      return result
    }

    var total = 0

    for (var i = 0; i < blocks.length; ++i) {
      total += blocks[i].difficulty
    }

    result.difficulty = Math.round(total / blocks.length)
    result.hashrate = Math.round(result.difficulty / 30)

    await this.setCache(cacheName, result, 30)

    return result
  }

  getLastBlockHeader () {
    return new Promise((resolve, reject) => {
      const cacheName = 'getLastBlockHeader'
      var obj

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => {
          if (this.usePoolMonitor) return this.query('SELECT `b`.*, `p`.`id` AS `poolId`, `p`.`name` AS `poolName`, `p`.`url` AS `poolURL` FROM `blocks` AS `b` LEFT JOIN `pool_blocks` AS `pb` ON `pb`.`hash` = `b`.`hash` LEFT JOIN `pools` AS `p` ON `p`.`id` = `pb`.`id` ORDER BY `b`.`height` DESC LIMIT 1', [])

          return this.query('SELECT * FROM `blocks` ORDER BY `height` DESC LIMIT 1', [])
        })
        .then(blocks => {
          if (blocks.length === 0) {
            return reject(new Error('No blocks found in backend storage'))
          }

          obj = blocks[0]
          obj.depth = 0
          obj.poolId = obj.poolId || false
          obj.poolName = obj.poolName || false
          obj.poolURL = obj.poolURL || false

          return this.query('SELECT COUNT(*) AS `transactionCount` FROM `transactions` WHERE `blockHash` = ?', [obj.hash])
        })
        .then(rows => { obj.transactionCount = (rows.length === 0) ? rows.length : rows[0].transactionCount })
        .then(() => { this.setCache(cacheName, obj, 5) })
        .then(() => { return resolve(obj) })
        .catch((error) => { return reject(error) })
    })
  }

  getBlockHeaderByHash (blockHash) {
    return new Promise((resolve, reject) => {
      const cacheName = 'getBlockHeaderByHash' + blockHash
      var topHeight
      var obj

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.getLastBlockHeader() })
        .then(block => { topHeight = block.height })
        .then(() => {
          if (this.usePoolMonitor) return this.query('SELECT `b`.*, `p`.`id` AS `poolId`, `p`.`name` AS `poolName`, `p`.`url` AS `poolURL` FROM `blocks` AS `b` LEFT JOIN `pool_blocks` AS `pb` ON `pb`.`hash` = `b`.`hash` LEFT JOIN `pools` AS `p` ON `p`.`id` = `pb`.`id` WHERE `b`.`hash` = ? LIMIT 1', [blockHash])

          return this.query('SELECT * FROM `blocks` WHERE `hash` = ? LIMIT 1', [blockHash])
        })
        .then(blocks => {
          if (blocks.length === 0) {
            return reject(new Error('Requested block not found'))
          }

          obj = blocks[0]
          obj.depth = topHeight - obj.height
          obj.poolId = obj.poolId || false
          obj.poolName = obj.poolName || false
          obj.poolURL = obj.poolURL || false

          return this.query('SELECT COUNT(*) AS `transactionCount` FROM `transactions` WHERE `blockHash` = ?', [obj.hash])
        })
        .then(rows => { obj.transactionCount = (rows.length === 0) ? rows.length : rows[0].transactionCount })
        .then(() => { this.setCache(cacheName, obj, 10) })
        .then(() => { return resolve(obj) })
        .catch((error) => { return reject(error) })
    })
  }

  getBlockHeaderByHeight (height) {
    return new Promise((resolve, reject) => {
      const cacheName = 'getBlockHeaderByHeight' + height
      var topHeight
      var obj

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.getLastBlockHeader() })
        .then(block => { topHeight = block.height })
        .then(() => {
          if (this.usePoolMonitor) return this.query('SELECT `b`.*, `p`.`id` AS `poolId`, `p`.`name` AS `poolName`, `p`.`url` AS `poolURL` FROM `blocks` AS `b` LEFT JOIN `pool_blocks` AS `pb` ON `pb`.`hash` = `b`.`hash` LEFT JOIN `pools` AS `p` ON `p`.`id` = `pb`.`id` WHERE `b`.`height` = ? LIMIT 1', [height])

          return this.query('SELECT * FROM `blocks` WHERE `height` = ? LIMIT 1', [height])
        })
        .then(blocks => {
          if (blocks.length === 0) {
            return reject(new Error('Requested block not found'))
          }

          obj = blocks[0]
          obj.depth = topHeight - obj.height

          return this.query('SELECT COUNT(*) AS `transactionCount` FROM `transactions` WHERE `blockHash` = ?', [obj.hash])
        })
        .then(rows => { obj.transactionCount = (rows.length === 0) ? rows.length : rows[0].transactionCount })
        .then(() => { this.setCache(cacheName, obj, 10) })
        .then(() => { return resolve(obj) })
        .catch((error) => { return reject(error) })
    })
  }

  getRecentChainStats () {
    return new Promise((resolve, reject) => {
      const cacheName = 'getRecentChainStats'

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => {
          return this.query([
            'SELECT `timestamp`, `difficulty`, `nonce`, `size`, ',
            '(SELECT COUNT(*) FROM `transactions` WHERE `blockHash` = `hash`) AS `txnCount` ',
            'FROM `blocks` ORDER BY `height` DESC ',
            'LIMIT 2880'
          ].join(''), [])
        })
        .then(blocks => {
          if (blocks.length === 0) {
            return reject(new Error('No blocks found'))
          }

          return this.setCache(cacheName, blocks, 30)
        })
        .then(blocks => { return resolve(blocks) })
        .catch((error) => { return reject(error) })
    })
  }

  getBlockHash (height) {
    return new Promise((resolve, reject) => {
      const cacheName = 'getBlockHash' + height

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.getBlockHeaderByHeight(height) })
        .then(block => { return this.setCache(cacheName, block.hash, 15) })
        .then(hash => { return resolve(hash) })
        .catch(error => { return reject(error) })
    })
  }

  getKeyImageUse (keyImage) {
    return new Promise((resolve, reject) => {
      const cacheName = 'getKeyImageUse' + keyImage

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => {
          return this.query(
            'SELECT keyImage, b.hash AS `block`, b.height AS `height`, b.`timestamp` AS `timestamp`, ' +
            'ti.txnHash AS `tx`, t.publicKey AS `publicKey`, t.fee AS `fee`, t.paymentID as `paymentID`, ' +
            't.unlockTime as `unlockTime`, amount FROM transaction_inputs AS ti ' +
            'LEFT JOIN transactions AS t ON t.txnHash = ti.txnHash ' +
            'LEFT JOIN blocks AS b on b.hash = t.blockHash WHERE keyImage = ?', [keyImage])
        })
        .then(image => { return this.setCache(cacheName, image, 15) })
        .then(data => { return resolve(data) })
        .catch(error => { return reject(error) })
    })
  }

  getBlockHeight (hash) {
    return new Promise((resolve, reject) => {
      const cacheName = 'getBlockHeight' + hash

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.getBlockHeaderByHash(hash) })
        .then(block => { return this.setCache(cacheName, block.height, 15) })
        .then(height => { return resolve(height) })
        .catch(error => { return reject(error) })
    })
  }

  getBlockCount () {
    return new Promise((resolve, reject) => {
      const cacheName = 'getBlockCount'

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.query('SELECT COUNT(*) AS `cnt` FROM `blocks`', []) })
        .then(results => {
          if (results.length !== 1) {
            return reject(new Error('Error when requesting total block count from backend database'))
          }

          return this.setCache(cacheName, results[0].cnt, 5)
        })
        .then(cnt => { return resolve(cnt) })
        .catch(error => { return reject(error) })
    })
  }

  getTransactionPool () {
    return new Promise((resolve, reject) => {
      const cacheName = 'getBlockCount'

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.query('SELECT * FROM `transaction_pool`', []) })
        .then(results => { return this.setCache(cacheName, results, 5) })
        .then(rows => { return resolve(rows) })
        .catch(error => { return reject(error) })
    })
  }

  getTransactionHashesByPaymentId (paymentId) {
    return new Promise((resolve, reject) => {
      const cacheName = 'getTransactionHashesByPaymentId' + paymentId

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => {
          return this.query([
            'SELECT `txnHash` AS `hash`,`mixin`,`timestamp`,`fee`,`size`, ',
            '`totalOutputsAmount` AS `amount` ',
            'FROM `transactions` ',
            'WHERE `paymentId` = ? ',
            'ORDER BY `timestamp`'].join(''), [paymentId])
        })
        .then(results => { return this.setCache(cacheName, results, 15) })
        .then(results => { return resolve(results) })
        .catch(error => { return reject(error) })
    })
  }

  getTransaction (hash) {
    return new Promise((resolve, reject) => {
      const cacheName = 'getTransaction' + hash
      var result = {}

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => {
          return this.query([
            'SELECT `transactions`.*, CAST(`unlockTime` AS CHAR) AS `unlockTimeString` ',
            'FROM `transactions` WHERE `transactions`.`txnHash` = ?'
          ].join(''), [hash])
        })
        .then(transactions => {
          if (transactions.length !== 1) {
            return reject(new Error('Transaction not found'))
          }

          const transaction = transactions[0]

          result.tx = {
            amount_out: transaction.totalOutputsAmount,
            fee: transaction.fee,
            hash: transaction.txnHash,
            mixin: transaction.mixin,
            paymentId: transaction.paymentId,
            size: transaction.size,
            extra: Buffer.from(transaction.extra, 'hex').toString(),
            unlock_time: transaction.unlockTimeString,
            nonce: transaction.nonce,
            publicKey: transaction.publicKey
          }

          return this.getBlockHeaderByHash(transaction.blockHash)
        })
        .then(block => {
          result.block = {
            cumul_size: block.size,
            difficulty: block.difficulty,
            hash: block.hash,
            height: block.height,
            timestamp: block.timestamp,
            tx_count: block.transactionCount
          }
        })
        .then(() => { return this.getTransactionInputs(result.tx.hash) })
        .then(inputs => { result.tx.inputs = inputs })
        .then(() => { return this.getTransactionOutputs(result.tx.hash) })
        .then(outputs => { result.tx.outputs = outputs })
        .then(() => { return this.getLastBlockHeader() })
        .then(header => { result.block.depth = header.height - result.block.height })
        .then(() => { return this.setCache(cacheName, result, 15) })
        .then(() => { return resolve(result) })
        .catch(error => { return reject(error) })
    })
  }

  getTransactionInputs (hash, trim) {
    if (typeof trim === 'undefined') trim = true

    function checkTrim (txns) {
      if (trim) {
        for (var i = 0; i < txns.length; i++) {
          delete txns[i].txnHash
        }
      }

      return txns
    }

    return new Promise((resolve, reject) => {
      const cacheName = 'getTransactionInputs' + hash

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.query('SELECT * FROM `transaction_inputs` WHERE `txnHash` = ? ORDER BY `amount`, `keyImage`', [hash]) })
        .then(results => {
          if (results.length === 0) {
            return resolve([])
          }

          for (var i = 0; i < results.length; i++) {
            results[i].type = results[i].type.toString(16).padStart(2, '0')
          }

          return this.setCache(cacheName, results, 60)
        })
        .then(results => { return checkTrim(results) })
        .then(results => { return resolve(results) })
        .catch(error => { return reject(error) })
    })
  }

  getTransactionOutputs (hash, trim) {
    if (typeof trim === 'undefined') trim = true

    function checkTrim (txns) {
      if (trim) {
        for (var i = 0; i < txns.length; i++) {
          delete txns[i].txnHash
        }
      }

      return txns
    }

    return new Promise((resolve, reject) => {
      const cacheName = 'getTransactionOutputs' + hash

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.query('SELECT * FROM `transaction_outputs` WHERE `txnHash` = ? ORDER BY `outputIndex`', [hash]) })
        .then(results => {
          if (results.length === 0) {
            return resolve([])
          }

          for (var i = 0; i < results.length; i++) {
            results[i].type = results[i].type.toString(16).padStart(2, '0')
          }

          return this.setCache(cacheName, results, 60)
        })
        .then(results => { return checkTrim(results) })
        .then(results => { return resolve(results) })
        .catch(error => { return reject(error) })
    })
  }

  getBlock (hash) {
    return new Promise((resolve, reject) => {
      const cacheName = 'getBlock' + hash
      var result
      var topHeight

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.getLastBlockHeader() })
        .then(header => {
          topHeight = header.height

          return this.getBlockHeaderByHash(hash)
        })
        .then(block => {
          result = block
          result.depth = topHeight - block.height

          return this.query([
            'SELECT `totalOutputsAmount` AS `amount_out`, `fee`, `txnHash` AS `hash`, `size` ',
            'FROM `transactions` WHERE `blockHash` = ?'
          ].join(''), [hash])
        })
        .then(transactions => {
          result.transactions = transactions

          return this.setCache(cacheName, result, 5)
        })
        .then(() => { return resolve(result) })
        .catch(error => { return reject(error) })
    })
  }

  getTransactionsByBlock (blockHash) {
    return new Promise((resolve, reject) => {
      const cacheName = 'getTransactionsByBlock' + blockHash

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => {
          return this.query([
            'SELECT `transactions`.*, CAST(`unlockTime` AS CHAR) AS `unlockTimeString` ',
            'FROM `transactions` WHERE `blockHash` = ?'
          ].join(''), [blockHash])
        })
        .then(rows => {
        /* If there are no rows, return immediately and do not
           cache the result just in case there was a delay in DB
           processing */
          if (rows.length === 0) return resolve([])

          return this.setCache(cacheName, rows, 120)
        })
        .then(rows => { return resolve(rows) })
        .catch(error => { return reject(error) })
    })
  }

  getBlocks (height, count) {
    const cnt = count || 30
    return new Promise((resolve, reject) => {
      const cacheName = 'getBlocks' + height + cnt

      /* We return just 30 blocks inclusive of our height */
      const min = height - (cnt - 1)
      const max = height

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => {
          return this.query([
            'SELECT `size`, `difficulty`, `b`.`hash`, `b`.`height`, `timestamp`, `nonce`, ',
            '(SELECT COUNT(*) FROM `transactions` WHERE `transactions`.`blockHash` = `b`.`hash`) AS `tx_count`, ',
            '`p`.`id` AS `poolId`, `p`.`name` AS `poolName`, `p`.`url` AS `poolURL` ',
            'FROM `blocks` AS `b` ',
            'LEFT JOIN `pool_blocks` AS `pb` ON `pb`.`hash` = `b`.`hash` ',
            'LEFT JOIN `pools` AS `p` ON `p`.`id` = `pb`.`id` ',
            'WHERE `b`.`height` BETWEEN ? AND ? ',
            'ORDER BY `b`.`height` DESC'].join(''), [min, max])
        })
        .then(blocks => {
          for (var i = 0; i < blocks.length; i++) {
            blocks[i].poolId = blocks[i].poolId || false
            blocks[i].poolName = blocks[i].poolName || false
            blocks[i].poolURL = blocks[i].poolURL || false
          }

          return blocks
        })
        .then(blocks => { return this.setCache(cacheName, blocks, 5) })
        .then(blocks => { return resolve(blocks) })
        .catch(error => { return reject(error) })
    })
  }

  getMixableAmounts (mixin) {
    mixin = mixin || 3

    return new Promise((resolve, reject) => {
      const cacheName = 'getMixableAmounts' + mixin

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => {
          return this.query([
            'SELECT `toim`.`amount`, `toim`.`globalIndex` + 1 AS `outputs`, `t`.`timestamp`, `b`.`height`, `t`.`txnHash`, `b`.`hash` ',
            'FROM `transaction_outputs_index_maximums` AS `toim` ',
            'LEFT JOIN `transaction_outputs` AS `to` ON `to`.`amount` = `toim`.`amount` AND `to`.`globalIndex` = ? ',
            'LEFT JOIN `transactions` AS `t` ON `t`.`txnHash` = `to`.`txnHash` ',
            'LEFT JOIN `blocks` AS `b` ON `b`.`hash` = `t`.`blockHash` ',
            'ORDER BY `toim`.`amount`'
          ].join(''), [mixin])
        })
        .then(results => {
          if (results.length === 0) {
            return resolve([])
          }

          return this.setCache(cacheName, results, 15)
        })
        .then(results => { return resolve(results) })
        .catch(error => { return reject(error) })
    })
  }

  getInfo () {
    return new Promise((resolve, reject) => {
      const cacheName = 'getInfo'

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.query('SELECT `payload` FROM `information` WHERE `key` = ?', ['getinfo']) })
        .then(results => {
          if (results.length === 0) {
            return reject(new Error('No record found'))
          }

          return this.setCache(cacheName, results[0].payload, 5)
        })
        .then(results => { return resolve(JSON.parse(results)) })
        .catch(error => { return reject(error) })
    })
  }

  getTransactionsStatus (hashes) {
    return new Promise((resolve, reject) => {
      const cacheName = 'getTransactionsStatus' + JSON.stringify(hashes)

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

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => {
          return this.query([
            'SELECT `txnHash` FROM `transaction_pool` ',
            'WHERE ' + criteria
          ].join(''), hashes)
        })
        .then(txns => {
          txns.forEach((txn) => {
            result.transactionsInPool.push(txn.txnHash)
          })

          return this.query([
            'SELECT `txnHash` FROM `transactions` ',
            'WHERE ' + criteria
          ].join(''), hashes)
        })
        .then(txns => {
          txns.forEach((txn) => {
            result.transactionsInBlock.push(txn.txnHash)
          })
        })
        .then(() => {
          hashes.forEach((txn) => {
            if (result.transactionsInPool.indexOf(txn) === -1 && result.transactionsInBlock.indexOf(txn) === -1) {
              result.transactionsUnknown.push(txn)
            }
          })
        })
        .then(() => { return this.setCache(cacheName, result, 5) })
        .then(() => { return resolve(result) })
        .catch(error => { return reject(error) })
    })
  }

  getNodeStats () {
    return new Promise((resolve, reject) => {
      const cacheName = 'getNodeStats'

      const nodeList = []
      const stamps = []

      function setNodePropertyValue (id, property, value) {
        for (var i = 0; i < nodeList.length; i++) {
          if (nodeList[i].id === id) {
            nodeList[i][property] = value
          }
        }
      }

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.query('SELECT * FROM `nodes` ORDER BY `name`', []) })
        .then(nodes => { nodes.forEach(node => nodeList.push(node)) })
        .then(() => { return this.query('SELECT `timestamp` AS `stamp` FROM `node_polling` GROUP BY `timestamp` ORDER BY `timestamp` DESC LIMIT 20', []) })
        .then(rows => { rows.forEach(row => stamps.push(row.stamp)) })
        .then(() => {
          const query = util.format('SELECT `id`, ((SUM(`status`) / COUNT(*)) * 100) AS `availability` FROM `node_polling` WHERE `timestamp` IN (%s) GROUP BY `id`', stamps.join(','))

          return this.query(query, [])
        })
        .then(rows => { rows.forEach(row => { setNodePropertyValue(row.id, 'availability', row.availability) }) })
        .then(() => { return this.query('SELECT MAX(`timestamp`) AS `timestamp` FROM `node_polling`', []) })
        .then(rows => {
          if (rows.length === 0) throw new Error('No timestamp information in the database')

          return this.query('SELECT * FROM `node_polling` WHERE `timestamp` = ?', [rows[0].timestamp || 0])
        })
        .then(rows => {
          rows.forEach((row) => {
            setNodePropertyValue(row.id, 'status', (row.status === 1))
            setNodePropertyValue(row.id, 'feeAddress', row.feeAddress || '')
            setNodePropertyValue(row.id, 'feeAmount', row.feeAmount)
            setNodePropertyValue(row.id, 'height', row.height)
            setNodePropertyValue(row.id, 'version', row.version)
            setNodePropertyValue(row.id, 'connectionsIn', row.connectionsIn)
            setNodePropertyValue(row.id, 'connectionsOut', row.connectionsOut)
            setNodePropertyValue(row.id, 'difficulty', row.difficulty)
            setNodePropertyValue(row.id, 'hashrate', row.hashrate)
            setNodePropertyValue(row.id, 'txPoolSize', row.txPoolSize)
            setNodePropertyValue(row.id, 'lastCheckTimestamp', row.timestamp)
          })
        })
        .then(() => {
          const query = util.format('SELECT `id`, `status`, `timestamp` FROM `node_polling` WHERE `timestamp` IN (%s) ORDER BY `id` ASC, `timestamp` DESC', stamps.join(','))

          return this.query(query, [])
        })
        .then(rows => {
          const temp = {}

          rows.forEach((row) => {
            if (!temp[row.id]) temp[row.id] = []
            temp[row.id].push({ timestamp: row.timestamp, status: (row.status === 1) })
          })

          Object.keys(temp).forEach((key) => {
            setNodePropertyValue(key, 'history', temp[key])
          })
        })
        .then(() => { return this.setCache(cacheName, nodeList, 60) })
        .then(results => { return resolve(results) })
        .catch(error => { return reject(error) })
    })
  }

  getPoolStats () {
    return new Promise((resolve, reject) => {
      const cacheName = 'getPoolStats'

      const poolList = []
      const stamps = []

      function setPoolPropertyValue (id, property, value) {
        for (var i = 0; i < poolList.length; i++) {
          if (poolList[i].id === id) {
            poolList[i][property] = value
          }
        }
      }

      this.checkCache(cacheName).then(cached => {
        if (cached) return resolve(cached)

        return this.query('SELECT * FROM `pools` ORDER BY `name`', [])
      }).then((pools) => {
        pools.forEach(node => poolList.push(node))

        return this.query('SELECT `timestamp` AS `stamp` FROM `pool_polling` GROUP BY `timestamp` ORDER BY `timestamp` DESC LIMIT 20', [])
      }).then((rows) => {
        rows.forEach(row => stamps.push(row.stamp))

        const query = util.format('SELECT `id`, ((SUM(`status`) / COUNT(*)) * 100) AS `availability` FROM `pool_polling` WHERE `timestamp` IN (%s) GROUP BY `id`', stamps.join(','))

        return this.query(query, [])
      }).then((rows) => {
        rows.forEach((row) => {
          setPoolPropertyValue(row.id, 'availability', row.availability)
        })

        return this.query('SELECT MAX(`timestamp`) AS `timestamp` FROM `pool_polling`', [])
      }).then((rows) => {
        if (rows.length === 0) throw new Error('No timestamp information in the database')
        return this.query('SELECT * FROM `pool_polling` WHERE `timestamp` = ?', [rows[0].timestamp || 0])
      }).then((rows) => {
        rows.forEach((row) => {
          setPoolPropertyValue(row.id, 'status', (row.status === 1))
          setPoolPropertyValue(row.id, 'height', row.height)
          setPoolPropertyValue(row.id, 'hashrate', row.hashrate)
          setPoolPropertyValue(row.id, 'miners', row.miners)
          setPoolPropertyValue(row.id, 'fee', row.fee)
          setPoolPropertyValue(row.id, 'minPayout', row.minPayout)
          setPoolPropertyValue(row.id, 'lastBlock', row.lastBlock)
          setPoolPropertyValue(row.id, 'donation', row.donation)
          setPoolPropertyValue(row.id, 'lastCheckTimestamp', row.timestamp)
        })

        const query = util.format('SELECT `id`, `status`, `timestamp` FROM `pool_polling` WHERE `timestamp` IN (%s) ORDER BY `id` ASC, `timestamp` DESC', stamps.join(','))

        return this.query(query, [])
      }).then((rows) => {
        const temp = {}

        rows.forEach((row) => {
          if (!temp[row.id]) temp[row.id] = []
          temp[row.id].push({ timestamp: row.timestamp, status: (row.status === 1) })
        })

        Object.keys(temp).forEach((key) => {
          setPoolPropertyValue(key, 'history', temp[key])
        })
      }).then(() => {
        return this.setCache(cacheName, poolList, 15)
      }).then((results) => {
        return resolve(results)
      }).catch((err) => {
        return reject(err)
      })
    })
  }

  getAmountKeys (amount, globalIndexes) {
    return new Promise((resolve, reject) => {
      if (!Array.isArray(globalIndexes)) return reject(new Error('Must supply an array of globalIndexes'))

      /* We have to do our own checking here because we are inserting them directly
         into the SQL statement */
      globalIndexes.forEach((num) => {
        if (isNaN(num)) return reject(new Error('All global indexes must be numers'))
      })

      const cacheName = 'getAmountKeys' + amount + JSON.stringify(globalIndexes)

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => {
          const query = util.format('SELECT `globalIndex`, `key` FROM `transaction_outputs` WHERE `amount` = ? AND `globalIndex` IN (%s) ORDER BY `globalIndex`', globalIndexes.join(','))

          return this.query(query, [amount])
        })
        .then(rows => {
          if (rows.length !== globalIndexes.length) {
            return reject(new Error('Data consistency error, could not provide all keys for supplied globalIndexes'))
          }

          return this.setCache(cacheName, rows, 60)
        })
        .then(rows => { return resolve(rows) })
        .catch(error => { return reject(error) })
    })
  }

  /* Heavyweight queries */

  findCurrentSyncHeight (knownBlockHashes, startHeight, startTimestamp) {
    startHeight = startHeight || 0
    startTimestamp = startTimestamp || 0
    return new Promise((resolve, reject) => {
      /* Insert data check? */
      const cacheName = 'findCurrentSyncHeight' + JSON.stringify(knownBlockHashes) + startHeight + startTimestamp
      var syncStart = 0

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => {
          if (knownBlockHashes.length > 0) {
            const criteria = []

            knownBlockHashes.forEach(hash => criteria.push(util.format('\'%s\'', hash)))

            const query = util.format('SELECT `height` FROM `blocks` WHERE `hash` IN (%s) ORDER BY `height` DESC LIMIT 1', criteria.join(','))

            return this.query(query)
          }
        })
        .then(rows => {
          if (rows && Array.isArray(rows) && rows.length === 1) {
            syncStart = rows[0].height + 1
          }

          if (startTimestamp > 0) {
            return this.query('SELECT `height` FROM `blocks` WHERE `timestamp` <= ? ORDER BY `height` DESC LIMIT 1', [startTimestamp])
          }
        })
        .then(rows => {
          if (rows && Array.isArray(rows) && rows.length === 1) {
            if (rows[0].height > syncStart) {
              syncStart = rows[0].height
            }
          }
        })
        .then(() => { if (startHeight > syncStart) syncStart = startHeight })
        .then(() => { return this.setCache(cacheName, syncStart, 60) })
        .then(result => { return resolve(result) })
        .catch(error => { return reject(error) })
    })
  }

  getWalletSyncDataByHeight (scanHeight, blockCount) {
    blockCount = checkBlockCount(blockCount)
    scanHeight = scanHeight || 0

    /* Go get the blocks from the scanHeight provided */
    return this.query([
      'SELECT `hash` AS `blockHash`, `height`, `timestamp`, `txnCount` FROM `blocks`',
      'LEFT JOIN (SELECT `blockHash`, COUNT(*) AS `txnCount` FROM `transactions` GROUP BY `blockHash`)',
      'AS `transactions` ON `transactions`.`blockHash` = `blocks`.`hash`',
      'WHERE `height` >= ? AND `height` < ? ORDER BY `height`'
    ].join(' '), [scanHeight, scanHeight + blockCount])
      .then(blocks => {
        const promises = []

        blocks.forEach((block) => {
          promises.push(this.buildWalletDataBlock(block))
        })

        return Promise.all(promises)
      })
      .then(blocks => {
        /* Sort the returned blocks because they could be out of order */
        blocks.sort((a, b) => (a.blockHeight > b.blockHeight) ? 1 : -1)

        return blocks
      })
  }

  getWalletSyncData (knownBlockHashes, blockCount) {
    blockCount = checkBlockCount(blockCount)

    if (!Array.isArray(knownBlockHashes)) throw new Error('You must supply an array of block hashes')
    if (knownBlockHashes.length === 0) throw new Error('You must supply at least one known block hash')

    /* Find out the highest block that we know about */
    return this.findCurrentSyncHeight(knownBlockHashes)
      .then(syncHeight => { return this.getWalletSyncDataByHeight(syncHeight, blockCount) })
  }

  legacyGetWalletSyncDataPreflight (startHeight, startTimestamp, blockHashCheckpoints, skipCoinbaseTransactions) {
    skipCoinbaseTransactions = skipCoinbaseTransactions || false

    if (!Array.isArray(blockHashCheckpoints)) throw new Error('You must supply an blockHashCheckpoints as an array')

    const result = {
      topBlock: {
        height: 0,
        hash: null
      },
      networkHeight: 0,
      height: 0,
      blockCount: 0,
      blockHashes: []
    }

    return this.getLastBlockHeader()
      .then(block => {
        result.topBlock.height = block.height
        result.topBlock.hash = block.hash
      })
      .then(() => { return this.findCurrentSyncHeight(blockHashCheckpoints, startHeight, startTimestamp) })
      .then(syncHeight => {
        const resolvableBlocks = result.topBlock.height - syncHeight + 1
        result.height = syncHeight
        result.blockCount = (resolvableBlocks > 100) ? 100 : resolvableBlocks

        /* Build out our query that we will run to find the blocks that we want data for */
        const blockQuery = [
          'SELECT `hash`, `height`, `timestamp`, `txnCount` FROM `blocks`',
          'LEFT JOIN (SELECT `blockHash`, COUNT(*) AS `txnCount` FROM `transactions` GROUP BY `blockHash`)',
          'AS `transactions` ON `transactions`.`blockHash` = `blocks`.`hash`',
          'WHERE `height` >= ? AND `height` <= ?'
        ]

        /* If we are skipping empty blocks, we need to make sure that the transaction count is
           greater than 1 as if the count is just 1, then it contains only the coinbase transaction */
        if (skipCoinbaseTransactions) {
          blockQuery.push('AND `transactions`.`txnCount` > 1')
        }

        blockQuery.push('ORDER BY `height` ASC LIMIT ?')

        return this.query(blockQuery.join(' '), [syncHeight, result.topBlock.height, result.blockCount])
      })
      .then(blocks => { blocks.forEach(block => { result.blockHashes.push(block.hash) }) })
      .then(() => { return result })
  }

  legacyGetWalletSyncDataLite (startHeight, blockCount) {
    blockCount = checkBlockCount(blockCount)

    return this.query([
      'SELECT `hash`, `height`, `timestamp`, `txnCount` FROM `blocks`',
      'LEFT JOIN (SELECT `blockHash`, COUNT(*) AS `txnCount` FROM `transactions` GROUP BY `blockHash`)',
      'AS `transactions` ON `transactions`.`blockHash` = `blocks`.`hash`',
      'WHERE `height` >= ? ORDER BY `height` ASC LIMIT ?'
    ].join(' '), [startHeight, blockCount])
      .then(rows => {
        const promises = []

        rows.forEach((block) => {
          promises.push(this.buildLegacyWalletDataBlock(block))
        })

        return Promise.all(promises)
      })
      .then(blocks => {
        /* Sort the returned blocks because they could be out of order */
        blocks.sort((a, b) => (a.blockHeight > b.blockHeight) ? 1 : -1)

        return blocks
      })
  }

  legacyGetWalletSyncData (startHeight, startTimestamp, blockHashCheckpoints, blockCount, skipCoinbaseTransactions) {
    blockCount = checkBlockCount(blockCount)
    skipCoinbaseTransactions = skipCoinbaseTransactions || false

    if (!Array.isArray(blockHashCheckpoints)) throw new Error('You must supply an blockHashCheckpoints as an array')

    var topBlock
    var topHeight = 0

    return this.getLastBlockHeader()
      .then(block => { topBlock = block })
      .then(() => { return this.findCurrentSyncHeight(blockHashCheckpoints, startHeight, startTimestamp) })
      .then(syncHeight => { topHeight = syncHeight })
      .then(() => {
        /* Build out our query that we will run to find the blocks that we want data for */
        const blockQuery = [
          'SELECT `hash`, `height`, `timestamp`, `txnCount` FROM `blocks`',
          'LEFT JOIN (SELECT `blockHash`, COUNT(*) AS `txnCount` FROM `transactions` GROUP BY `blockHash`)',
          'AS `transactions` ON `transactions`.`blockHash` = `blocks`.`hash`',
          'WHERE `height` >= ? AND `height` <= ?'
        ]

        /* If we are skipping empty blocks, we need to make sure that the transaction count is
           greater than 1 as if the count is just 1, then it contains only the coinbase transaction */
        if (skipCoinbaseTransactions) {
          blockQuery.push('AND `transactions`.`txnCount` > 1')
        }

        blockQuery.push('ORDER BY `height` ASC LIMIT ?')

        return this.query(blockQuery.join(' '), [topHeight, topBlock.height, blockCount])
      })
      .then(blocks => {
        /* Loop through the blocks and build out the data we need */
        const promises = []
        blocks.forEach((block) => {
          promises.push(this.buildLegacyWalletDataBlock(block, skipCoinbaseTransactions))
        })

        return Promise.all(promises)
      })
      .then(blocks => {
        /* Sort the returned blocks because they could be out of order */
        blocks.sort((a, b) => (a.blockHeight > b.blockHeight) ? 1 : -1)

        return {
          blocks: blocks,
          from: topHeight,
          topBlock: topBlock
        }
      })
  }

  buildWalletDataBlock (block) {
    return new Promise((resolve, reject) => {
      const cacheName = block.blockHash

      const obj = {
        blockHash: block.blockHash,
        height: block.height,
        timestamp: block.timestamp,
        transactions: []
      }

      const transactionMap = {}

      /* Helper method that's used below */
      function getError (hash, message) {
        return util.format('Internal Data Consistency Error [%s]: %s', hash, message)
      }

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.getTransactionsByBlock(block.blockHash) })
        .then(txns => {
          txns.forEach((txn) => {
            transactionMap[txn.txnHash] = {
              hash: txn.txnHash,
              publicKey: txn.publicKey,
              unlockTime: txn.unlockTimeString,
              paymentId: txn.paymentId,
              inputs: [],
              outputs: [],
              meta: {
                inputsTotal: txn.totalInputsAmount,
                outputsTotal: txn.totalOutputsAmount,
                fee: txn.fee
              }
            }
          })
        })
        .then(() => {
          const promises = []

          Object.keys(transactionMap).forEach((hash) => {
            promises.push(this.getTransactionInputs(hash, false))
          })

          return Promise.all(promises)
        })
        .then(results => {
          /* Loop through all the results from the promises */
          results.forEach((result) => {
            /* Loop through all of the inputs in the result */
            result.forEach((input) => {
              transactionMap[input.txnHash].inputs.push({
                keyImage: input.keyImage,
                amount: input.amount,
                type: input.type
              })
            })
          })
        })
        .then(() => {
          /* Loop through the transactions and get their outputs */
          const promises = []

          Object.keys(transactionMap).forEach((hash) => {
            promises.push(this.getTransactionOutputs(hash, false))
          })

          return Promise.all(promises)
        })
        .then(results => {
          /* Loop through all the results from the promises */
          results.forEach((result) => {
            /* Loop through all the outputs in the result */
            result.forEach((output) => {
              transactionMap[output.txnHash].outputs.push({
                index: output.outputIndex,
                globalIndex: output.globalIndex,
                key: output.key,
                amount: output.amount,
                type: output.type
              })
            })
          })
        })
        .then(() => {
          /* Now we need to go through our transaction map
             and toss them on to the response transactions array */
          Object.keys(transactionMap).forEach((key) => {
            obj.transactions.push(transactionMap[key])
          })
        })
        .then(() => {
          /* We need to perform some internal consistency checking */

          /* If we don't have the same number of transactions as we expected
             from the information in the block data, then we have a problem. */
          if (block.txnCount !== obj.transactions.length) {
            return reject(new Error(getError(block.hash, 'Unexpected number of transactions in structure')))
          }

          /* Loop through the transactions and verify that we have all
             of the transactions inputs and outputs and that everything
             makes sense */
          for (var i = 0; i < obj.transactions.length; i++) {
            const txn = obj.transactions[i]

            /* If we have no inputs and no outputs then something went
               terribly wrong */
            if (txn.inputs.length === 0 && txn.outputs.length === 0) {
              return reject(new Error(getError(txn.hash, 'No inputs and no outputs')))
            }

            /* If this is not a coinbase transaction we do extra checks */
            if (txn.meta.inputsTotal !== 0) {
              /* Tally the total amount of our inputs */
              var inputsTotal = 0
              txn.inputs.forEach((input) => {
                inputsTotal += input.amount
              })

              /* If the total amount of the inputs does not match the meta information,
                 this is an error */
              if (inputsTotal !== txn.meta.inputsTotal) {
                return reject(new Error(getError(txn.hash, 'Inputs total does not match meta information')))
              }
            }

            /* Tally the total amount of our outputs */
            var outputsTotal = 0
            txn.outputs.forEach((output) => {
              outputsTotal += output.amount
            })

            /* If the total amount of the outputs does not match the meta information,
               this is an error */
            if (outputsTotal !== txn.meta.outputsTotal) {
              return reject(new Error(getError(txn.hash, 'Outputs total does not match meta information')))
            }

            /* If this is not a coinbase transaction we do extra checks */
            if (txn.meta.inputsTotal !== 0) {
              /* If the total inputs minus the total outputs does not match the transaction
                 fee from the meta information, this is an error */
              if (inputsTotal - outputsTotal !== txn.meta.fee) {
                return reject(new Error(getError(txn.hash, 'Transaction fee does not match')))
              }
            }

            /* Delete the meta information from the transaction information */
            delete obj.transactions[i].meta
          }
        })
        .then(() => { return this.setCache(cacheName, obj, 60 * 60 * 24) })
        .then(response => { return resolve(response) })
        .catch(error => { return reject(error) })
    })
  }

  buildLegacyWalletDataBlock (block, skipCoinbaseTransactions) {
    return new Promise((resolve, reject) => {
      const cacheName = 'legacy' + block.hash

      /* Set up our base object */
      const obj = {
        blockHash: block.hash,
        blockHeight: block.height,
        blockTimestamp: block.timestamp,
        coinbaseTX: {},
        transactions: []
      }

      const transactionMap = {}

      /* Helper method that's used below */
      function getError (hash, message) {
        return util.format('Internal Data Consistency Error [%s]: %s', hash, message)
      }

      this.checkCache(cacheName)
        .then(cached => { if (cached) return resolve(cached) })
        .then(() => { return this.getTransactionsByBlock(block.hash) })
        .then(txns => {
          txns.forEach((txn) => {
            if (txn.totalInputsAmount === 0) {
              obj.coinbaseTX.hash = txn.txnHash
              obj.coinbaseTX.txPublicKey = txn.publicKey
              obj.coinbaseTX.unlockTime = txn.unlockTimeString
              obj.coinbaseTX.outputs = []
            } else {
              transactionMap[txn.txnHash] = {
                hash: txn.txnHash,
                inputs: [],
                outputs: [],
                paymentID: txn.paymentId,
                txPublicKey: txn.publicKey,
                unlockTime: txn.unlockTimeString,
                meta: {
                  inputsTotal: txn.totalInputsAmount,
                  outputsTotal: txn.totalOutputsAmount,
                  fee: txn.fee
                }
              }
            }
          })

          return this.getTransactionOutputs(obj.coinbaseTX.hash)
        })
        .then(outputs => {
          /* Loop through the coinbase transaction outputs and populate the object */
          outputs.forEach((output) => {
            obj.coinbaseTX.outputs.push({
              amount: output.amount,
              key: output.key,
              globalIndex: output.globalIndex
            })
          })
        })
        .then(() => {
          /* Loop through the transactions and get their inputs */
          const promises = []

          Object.keys(transactionMap).forEach((hash) => {
            promises.push(this.getTransactionInputs(hash, false))
          })

          return Promise.all(promises)
        })
        .then(results => {
          /* Loop through all the results from the promises */
          results.forEach((result) => {
            /* Loop through all of the inputs in the result */
            result.forEach((input) => {
              transactionMap[input.txnHash].inputs.push({
                amount: input.amount,
                k_image: input.keyImage
              })
            })
          })
        })
        .then(() => {
          /* Loop through the transactions and get their outputs */
          const promises = []

          Object.keys(transactionMap).forEach((hash) => {
            promises.push(this.getTransactionOutputs(hash, false))
          })

          return Promise.all(promises)
        })
        .then(results => {
          /* Loop through all the results from the promises */
          results.forEach((result) => {
            /* Loop through all the outputs in the result */
            result.forEach((output) => {
              transactionMap[output.txnHash].outputs.push({
                amount: output.amount,
                key: output.key,
                globalIndex: output.globalIndex
              })
            })
          })
        })
        .then(() => {
          /* Now we need to go through our transaction map
             and toss them on to the response transactions array */
          Object.keys(transactionMap).forEach((key) => {
            obj.transactions.push(transactionMap[key])
          })
        })
        .then(() => {
          /* We need to perform some internal consistency checking */

          /* If the coinbase transaction has no ouputs, that's a problem */
          if (obj.coinbaseTX.outputs.length === 0) {
            return reject(new Error(getError(block.hash, 'Coinbase Transaction Has No Outputs')))
          }

          /* If we don't have the same number of transactions as we expected
             from the information in the block data, then we have a problem.
             We have to add one here because we need to include the coinbase
             transaction in our count */
          if (block.txnCount !== obj.transactions.length + 1) {
            return reject(new Error(getError(block.hash, 'Unexpected number of transactions in structure')))
          }

          /* Loop through the transactions and verify that we have all
             of the transactions inputs and outputs and that everything
             makes sense */
          for (var i = 0; i < obj.transactions.length; i++) {
            const txn = obj.transactions[i]

            /* If we have no inputs and no outputs then something went
               terribly wrong */
            if (txn.inputs.length === 0 && txn.outputs.length === 0) {
              return reject(new Error(getError(txn.hash, 'No inputs and no outputs')))
            }

            /* Tally the total amount of our inputs */
            var inputsTotal = 0
            txn.inputs.forEach((input) => {
              inputsTotal += input.amount
            })

            /* If the total amount of the inputs does not match the meta information,
               this is an error */
            if (inputsTotal !== txn.meta.inputsTotal) {
              return reject(new Error(getError(txn.hash, 'Inputs total does not match meta information')))
            }

            /* Tally the total amount of our outputs */
            var outputsTotal = 0
            txn.outputs.forEach((output) => {
              outputsTotal += output.amount
            })

            /* If the total amount of the outputs does not match the meta information,
               this is an error */
            if (outputsTotal !== txn.meta.outputsTotal) {
              return reject(new Error(getError(txn.hash, 'Outputs total does not match meta information')))
            }

            /* If the total inputs minus the total outputs does not match the transaction
               fee from the meta information, this is an error */
            if (inputsTotal - outputsTotal !== txn.meta.fee) {
              return reject(new Error(getError(txn.hash, 'Transaction fee does not match')))
            }

            /* Delete the meta information from the transaction information */
            delete obj.transactions[i].meta
          }
        })
        .then(() => { return this.setCache(cacheName, obj, 60 * 60 * 24) })
        .then(response => {
          /* If we were told to skip coinbase transactions, delete it from
             the return object */
          if (skipCoinbaseTransactions) {
            delete response.coinbaseTX
          }

          return resolve(response)
        })
        .catch(error => { return reject(error) })
    })
  }

  getRandomOutputsForAmounts (amounts, mixin) {
    const that = this

    if (!Array.isArray(amounts)) throw new Error('You must supply an array of amounts')
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

    /* Go get the maximum globalIndex values for each of the
       amounts we want mixins for */
    return this.query([
      'SELECT `amount`, `globalIndex` ',
      'FROM `transaction_outputs_index_maximums` ',
      'WHERE ' + criteria
    ].join(''), dedupedAmounts)
      .then(async function (results) {
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
        return that.query([
          'SELECT `amount`, `globalIndex` AS `global_amount_index`, `key` AS `out_key` ',
          'FROM `transaction_outputs` WHERE ' + randomCriteria + ' ',
          'ORDER BY `amount` ASC'
        ].join(''), randomValues)
      })
      .then(results => {
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
      })
  }
}

function checkBlockCount (blockCount) {
  blockCount = blockCount || 100
  blockCount = Math.abs(blockCount)
  blockCount = (blockCount > 100) ? 100 : blockCount

  return blockCount
}

/* Relatively lightweight queries */

module.exports = DatabaseBackend

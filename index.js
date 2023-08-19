
const express = require('express');
const mysql = require('mysql2');
const bodyParser = require('body-parser');
const redis = require('ioredis');
require('dotenv').config()

const app = express();



const commonResponse = function (data, error) {
    if (error) {
        return {
            success: false,
            error: error
        }
    }

    return {
        success: true,
        data: data
    }
};

const redisCon = new redis({
    host: process.env.REDIS_HOST,
    port: process.env.REDIS_PORT
});

const mysqlCon = mysql.createConnection({
    host: process.env.MYSQL_HOST,
    port: process.env.MYSQL_PORT,
    user: process.env.MYSQL_USER,
    password: process.env.MYSQL_PASS,
    database: process.env.MYSQL_DB
});

const query = (query, values) => {
    return new Promise((resolve, reject) => {
        mysqlCon.query(query, values, (err, result, fields) => {
            if (err) {
                reject(err)
            } else {
                resolve(result)
            }
        })
    })
};

mysqlCon.connect((err) => {
    if (err) throw err

    console.log("mysql successfully connected")
});

app.use(bodyParser.json());

app.get('/user', (request, response) => {
    mysqlCon.query("select * from revou.user", (err, result, fields) => {
        if (err) {
            response.status(500).json(commonResponse(null, "server error"))
            response.end()
            return
        }

        response.status(200).json(commonResponse(result, null))
        response.end()
    })


});



app.get("/user/:id", async (request, response) => {
    try {
        const id = request.params.id
        const userKey = "user:" + id
        const cacheData = await redisCon.hgetall(userKey)

        if (Object.keys(cacheData).length !== 0) {
            console.log("get data from cache")
            response.status(200).json(commonResponse(cacheData, null))
            response.end()

            return
        }

        const dbData = await query(`SELECT
        u.id,
        u.name,
        u.address,
        SUM(CASE WHEN t.type = 'income' THEN t.amount ELSE -t.amount END) AS balance,
        SUM(CASE WHEN t.type = 'expense' THEN t.amount ELSE 0 END) AS expanse
        FROM
            revou.user u
        LEFT JOIN
            revou.transaction t ON u.id = t.user_id
        WHERE
            u.id = ?
        GROUP BY
        u.id;`, id)

        await redisCon.hset(userKey, dbData[0])
        await redisCon.expire(userKey, 20);

        response.status(200).json(commonResponse(dbData[0], null))
        response.end()
    } catch (err) {
        console.error(err)
        response.status(500).json(commonResponse(null, "server error"))
        response.end()
        return
    }

});

app.post("/transaction", async (request, response) => {
    try {
        const body = request.body
        const dbData = await query(`insert into 
            revou.transaction (user_id , type, amount)
            values 
            (?,?,?)`, [body.user_id, body.type, body.amount])

        const personId = body.user_id
        const userKey = "user:" + personId
        await redisCon.del(userKey)

        response.status(500).json(commonResponse({
            id: dbData.insertId
        }, null))
        response.end()
    } catch (err) {
        console.error(err)
        response.status(500).json(commonResponse(null, "server error"))
        response.end()
        return

    }
});

app.delete("/transaction/:id", async (request, response) => {
    try {
        const id = request.params.id
        const data = await query("select user_id from revou.transaction where id = ?", id)
        if (Object.keys(data).length === 0) {
            response.status(404).json(commonResponse(null, "data not found"))
            response.end()
            return
        }
        const personId = data[0].user_id
        const userKey = "user:" + personId
        await query(`delete from revou.transaction where id =?`)
        await redisCon.del(userKey)

        response.status(200).json(commonResponse({
            id: id
        }))

        response.end()
    } catch (err) {
        console.error(err)
        response.status(500).json(commonResponse(null, "server error"))
        response.end()
        return
    }
});

app.put("/transaction/:transactionId", async (request, response) => {
    try {
        const transactionId = request.params.transactionId;
        const body = request.body;


        const dbData = await query(`
      UPDATE revou.transaction 
      SET user_id = ?, type = ?, amount = ? 
      WHERE id = ?`,
            [body.user_id, body.type, body.amount, transactionId]
        );

        const userId = body.user_id;
        const userKey = "user:" + userId;
        await redisCon.del(userKey);

        response.status(500).json(commonResponse({
            id: dbData.insertId
        }, null))
        response.end()
    } catch (err) {
        console.error(err)
        response.status(500).json(commonResponse(null, "server error"))
        response.end()
        return
    }
});





app.listen(3000, () => {
    console.log("running in 3000")
});


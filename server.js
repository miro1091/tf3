const express = require('express');
const tf = require('@tensorflow/tfjs');
require('@tensorflow/tfjs-node');
const _ = require('lodash');
const mysql = require('mysql');
const bodyParser = require('body-parser')

const app = express();

app.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.json())

const connection = mysql.createConnection({
    host: 'jucondbs-cluster-1.cluster-croyakapeixc.eu-central-1.rds.amazonaws.com',
    user: 'web',
    password: 'Opj126Aml[',
    database: 'dc_nr'
  });

connection.connect((err) => {
    if (err) throw err;
    console.log('Connected!');
});

app.get('/', (req, res) => {
    res.send('machine learning app');
});


app.get('/api/generate', (req, res) => {
    res.send('machine learning is loading...');
    neuralNetwork();
});

//http://localhost:3000/api/query?dtm=1542289461&kid=[1,14,27,5,31]
app.post('/api/query', (req,res)=>{
    console.log(req.query);
    res.status(200).json({message:"success"});
    return 0;
})

// {
// 	"dtm":"1542289461", - timestamp
// 	"kid":[1,14,27,5,31]
// }
app.post('/api/json', (req,res)=>{
    const jsonData = req.body;

    if(!_.isEmpty(jsonData) && jsonData.hasOwnProperty('dtm') && jsonData.hasOwnProperty('kid')){
        neuralNetwork(jsonData);
        res.status(200).json({message:"success"});
    }else{
        res.status(400).json({message:"object is empty or properties are not defined properly"});
    }
    return 0;
})

app.listen(process.env.PORT || 3000);

const neuralNetwork = (jsonData = {}) => {

    Promise.all([fetchDbDataAll(), fetchDbDataSpecific(jsonData)]).then((dbData)=>{

        const dbDataAll = dbData[0];
        let dbDataSpecific = dbData[1];

        if(dbDataSpecific === '' || dbDataSpecific === undefined || dbDataSpecific === null){
            dbDataSpecific = dbDataAll;
        }

        const testingDataTensor = tf.tensor2d(dbDataSpecific.map(item => [
            item.kat_id,
            item.year_birthdate,
            item.month_o_dtm,
            item.hour_o_cas,
            item.k_id
        ]));

        let categoriesInput = dbDataAll.map(item => item.s_id);
        let categoriesOutput = dbDataSpecific.map(item => item.k_id);

        categoriesInput = _.uniq(categoriesInput);

        trainedModel().then((modelLoaded)=>{
            splitLoadedData(modelLoaded.predict(testingDataTensor).dataSync(), categoriesInput, categoriesOutput);
        });

    });

    return;

}

const fetchDbDataAll = () => {
        return new Promise((resolve, reject) => {
            
                connection.query("SELECT t_id, kat_id, s_id, p_id, YEAR(k_datum_narodenia) year_birthdate, MONTH(k_datum_narodenia) month_birthdate, MONTH('2018-10-24') month_o_dtm, WEEKDAY('2018-10-24') weekday_o_dtm, 9 AS hour_o_cas, k_id from dwh_all where YEAR(k_datum_narodenia) < 2000 and YEAR(k_datum_narodenia) > 0", (err,res) => {
                    if(err){
                        reject();
                    } 
                    resolve(res);
                })    
        })
}

const fetchDbDataSpecific = (jsonData) => {

    if(!_.isEmpty(jsonData) && jsonData.hasOwnProperty('dtm') && jsonData.hasOwnProperty('kid')){
        const date = new Date(Number(jsonData.dtm)*1000);
        const dateString = date.getFullYear() + "-" +(date.getMonth() + 1) + "-" + date.getDate();
        const hourString = date.getHours();
        const kidString = jsonData.kid.toString();

        return new Promise((resolve, reject) => {
            
                connection.query("SELECT t_id, kat_id, s_id, p_id, YEAR(k_datum_narodenia) year_birthdate, MONTH(k_datum_narodenia) month_birthdate, MONTH('"+dateString+"') month_o_dtm, WEEKDAY('"+dateString+"') weekday_o_dtm, "+hourString+" AS hour_o_cas, k_id from dwh_all where YEAR(k_datum_narodenia) < 2000 and YEAR(k_datum_narodenia) > 0 and k_id in ("+kidString+")", (err,res) => {
                    if(err){
                        reject();
                    } 
                    resolve(res);
                })    
        })
    }
    
    return null;
    
}

async function trainedModel() {
    return await tf.loadModel('file://./models/nr/s_id/model.json');
}

const splitLoadedData = (loadedData, categoriesInput, categoriesOutput) => {
    let dbInsertArray = [];
    const categoriesInputLength = categoriesInput.length;

    for (let i = 0, j = 0; i < loadedData.length; i += categoriesInputLength, j++) {

        categoriesInputChunk = loadedData.slice(i, i + categoriesInputLength);

        dbInsertArray = [];
        categoriesInputChunk.map((item,z) => {  
            if(categoriesOutput[j] !== null && categoriesOutput[j] !== '') {
                dbInsertArray.push([categoriesInput[z],categoriesOutput[j],item]);
            }
        });

        try{
            connection.query('insert into ai_results_sluzba (id_sluzba,id_klient_input,probability) values ?', [dbInsertArray], (err,res) => {
                if(err) throw err;
                console.log('chunk inserted: ' + j);
            })
        }catch(err){
            //or try to connect to db again
            continue;
        }

    }
}
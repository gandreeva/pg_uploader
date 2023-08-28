const util = require('./lib/new_object')
const csv2array = require('./lib/csv2array')
const sql = require('./lib/create_table')
const pg = require('pg');
const { Integer } = require('read-excel-file');
require('pg-essential').patch(pg);

// Uploading googleplaystore statistics

var data_dir, mask;;
var table_name = {table: 'rfm_analysis', schema: 'import_data'}

const obj_desc = {
    'App':{	prop: 'App', type: String},
    'Category':{	prop: 'Category', type: String},
    'Rating':{	prop: 'Rating', type: String},
    'Reviews':{	prop: 'Reviews', type: String},
    'Size':{	prop: 'Size', type: String},
    'Installs':{	prop: 'Installs', type: String},
    'Type':{	prop: 'Type', type: String},
    'Price':{	prop: 'Price', type: String},
    'Content Rating':{	prop: 'Content_Rating', type: String},
    'Genres':{	prop: 'Genres', type: String},
    'Last Updated':{	prop: 'Last_Updated', type: String},
    'Current Ver':{	prop: 'Current_Ver', type: String},
    'Android Ver':{	prop: 'Android_Ver', type: String},
    };


const args = process.argv.slice(2); 
const [dir_name, file_mask ] = args;  
if (dir_name === undefined) {   
    console.error('Please pass a directory with data');   
    process.exit(0); 
} else{
    data_dir = dir_name;
    mask = file_mask;
    console.log(`data_dir: ${data_dir}`); 
    console.log(`file mask: ${mask}`); 
}

/* const client = new pg.Client({
    host: '127.0.0.1',
    port: 5432,
    database: 'data_analysis',
    user: 'postgres',
    password: '33rjhjds',
  }); */

const client = new pg.Client({
    host: '172.17.0.2',
    port: 5432,
    database: 'tutorial',
    user: 'postgres',
    password: 'password',
    application_name: 'JS DATA Loader'
  });



async function runTask1() {
    // read all csv in specified directory whioj set in var 'data_dir'
    const csv_data =await csv2array.getData(data_dir, obj_desc, mask);
    //console.log(csv_data);
    console.log("read dir done");
    return csv_data;
}


async function runTask2() {

    await client.connect()

    const result1 = await client.query('SELECT NOW() as now')
    console.log(`DB server time: ${result1.rows[0]['now']}`) // get result from query

    //create Table in DB
     createTableQuery = sql.createQuery(table_name, obj_desc,true);

    //
     console.log(`create query: ${createTableQuery}`) // get result from query
     const result2 = await client.query(createTableQuery);

     return Promise.resolve(1)
}


async function runTask() {
    // prepare task before insert data
    tasks = [];
    tasks.push( runTask1() );
    tasks.push( runTask2() );

    return Promise.all(tasks);
}


async function runInsertTask(bulkData) {
    let columns = util.objHelper.getColumns(obj_desc);

    console.log(`records to insert: ${bulkData.length}`)

    await client.executeBulkInsertion(bulkData, columns, table_name );
    console.log("data succefully uploaded");
    
    return Promise.resolve(1)
    
}

runTask()
.then((data) => {
    if(data){
        console.log('prepare task done')
        // 0 - index of first task result in Promise.All
        if(Array.isArray(data[0])){
          return runInsertTask(data[0])
        }else{
          console.log('Nothing to insert')
          return Promise.resolve(3)
        }
        
    }
  //return 1;
})
.catch(e => console.error(e.stack))
.then(() => {
  console.log('connection closed')
  client.end()
})

'use strict';

const npm = {
  XLSX: require('xlsx'),
  fs: require('fs'),
  path: require('path'),
  parse: require('csv-parse'),
  readXlsxFile: require('read-excel-file/node'),
  util: require('./new_object'),
  sqlObj: require('./create_table')
};
// schema for reading excell file


const helper = {

  file_msexcell_ext: ['.xlsx', '.xls'],

  getFiles: function (data_dir, files_, file_mask) {

    let dir = npm.path.join(__dirname, '..', data_dir);
    console.log(dir);
    files_ = files_ || [];
    var files = npm.fs.readdirSync(dir);
    for (var i in files) {
      var name = npm.path.join(dir,files[i]);
      if (npm.fs.statSync(name).isDirectory()) {
        this.getFiles(npm.path.join(data_dir,files[i]), files_, file_mask);
      } else {
        // math with file_name
        if((file_mask && name.match(file_mask)) || !file_mask)
          files_.push(name);
      }
    }
    return files_;
  },

  getFileExtension: function (file) {
    return npm.path.extname(file);
  },

  isExcelFile: function(file_ext){
    return this.file_msexcell_ext.find(curr_ext => file_ext === curr_ext);
  },


  mergeTaskResult: function (dataArray) {
    let oneDimData = []
    dataArray.map((line) => {
      if (Array.isArray(line))
        line.map(values => {
          oneDimData.push(values);
        })
    })
    return oneDimData;
  }

};





async function readExcelFile(data_struct, file) {

  let columns =  Object.keys(data_struct);
  let schema = {};
  Object.keys(data_struct).forEach(field_name => {
    schema[  field_name ] = { "prop": field_name, "type": data_struct[ field_name ].type };
  });


  return new Promise(function (resolve, reject) {
    let ref_data = [];
    let workbook = npm.XLSX.read(file, { type: 'file' });
    let file_name = npm.path.basename(file);
    const [firstSheetName] = workbook.SheetNames;
    const worksheet = workbook.Sheets[firstSheetName];

    const rows = npm.XLSX.utils.sheet_to_json(worksheet, {
      raw: false, // Use raw values (true) or formatted strings (false)
      header: 1, // Generate an array of arrays ("2D Array")
    });

    console.log(`file :${file} has been read` );

    let header = rows[ 0 ];
    let col_ind = [];
     
   
    col_ind  = columns.map((value, index) => {

      return  header.indexOf(value);

    });


    let prepared_data = rows.map((row, r_ind)=>{
      if (r_ind > 0 ){ // skip header
        
        let objProp = {};

        col_ind.forEach( (value, index) =>{
          let dataType = data_struct[ Object.keys(data_struct)[ index] ];
          if( value == -1){
              if (dataType["value"] !== undefined) objProp[ dataType.prop ] = {value: dataType.value };
          }else{
                // should we transform data element
            let el = row[ col_ind[ index ] ];    
            objProp[ dataType.prop ] = {value: dataType['parse'] ? dataType.parse(el) : el};
          }
  
        } );

        /* old code version
        row.map((el, d_ind) => {
          let loc_ind = col_ind[d_ind];
          if( loc_ind >= 0 ) {
            let dataType = data_struct[ Object.keys(data_struct)[loc_ind] ];
          // should we transform data element
            objProp[ dataType.prop ] = {value: dataType['parse'] ? dataType.parse(el) : el};
          }      
        });
        */
        // add source name into data set
       objProp[ "file_name" ] = {value: file_name};
        return Object.create(null, objProp);
      }

    })

    resolve(prepared_data.slice(1 ));
  });
};


// read CSV file
async function readFile(obj_desc, file) {
  let csvData = [];
  let col_ind = [];
  return new Promise(function (resolve, reject) {
    
    npm.fs.createReadStream(file)
      .pipe(npm.parse({
        delimiter: ','
      }))
      .on('data', function (csvrow) {

        //save raw into inner array or do something with csvrow
        // convertion routine
       
        if (this.info.records == 1){ //head  line
          col_ind = csvrow.map((value, index) => {

            return Object.keys(obj_desc).indexOf(value);

          });

        } else{ // read data line by line 
          let convData = [];
          csvrow.forEach((el, index)=>{
            let loc_ind = col_ind[index];
            if( loc_ind >= 0 ) {
              let dataType = obj_desc[ Object.keys(obj_desc)[loc_ind] ];
              // should we transform data element
              convData[ loc_ind ] = dataType['parse'] ? dataType.parse(el) : el;
            }
           } );
           
          csvData.push(convData);
        }  
        //do something wiht csvData
        //console.log(csvData);
      })
      .on('end', () => {
        // column name from the head of csv file
        let data = [];
        let data_struct = obj_desc ? obj_desc : csvData[0];
        for (var i = 1; i < csvData.length; i++) {
          // create data array
          //console.log(csvData[i]);
          let obj = npm.util.objHelper.createEmptyObj(data_struct);
          npm.util.objHelper.setValues(obj, csvData[i]);
          data.push(obj);
        }
       // console.log(npm.fs.stat());
        // Promise resolve
        resolve(data);
      });
  });
};

async function readAll(data_dir, obj_desc,file_mask) {
  let read_tasks = [];

  var files = helper.getFiles(data_dir,null, file_mask); // file name's

  for (var f = 0; f < files.length; f++) {
    console.log(`read file_${f+1}: ${files[f]}`);
    let file_ext = helper.getFileExtension(files[f]);

    if (helper.isExcelFile(file_ext)) {

      read_tasks.push(readExcelFile(obj_desc, files[f]));
    } else if (file_ext === '.txt' || file_ext === '.csv') {
      read_tasks.push(readFile(obj_desc, files[f]));

    }
  }
  return Promise.all(read_tasks);
};

async function getData(data_dir, obj_desc, file_mask) {
  npm.util.objHelper.initObjDesc(obj_desc);
  let csv_data = await readAll(data_dir, obj_desc, file_mask);
  return helper.mergeTaskResult(csv_data);
};

const exp = {
  getData
}

Object.freeze(exp);

module.exports = exp;
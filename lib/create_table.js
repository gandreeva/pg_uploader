const format = require("string-template")
let queryTemplate1 = 'drop table IF EXISTS {table_name}';
let queryTemplate2 = 'CREATE TABLE IF NOT EXISTS {table_name} ( {columns_desc} )';
let column_type = '{column_name} {column_type} {primary_key} {not_null}';
let numeric_re = /(^numeric)(?=\((\d*,\d*)\))/;
let empty = '';

let Numeric = function (precision, scale){
    return {
        type: `numeric(${precision},${scale})`,
        precision: precision,
        scale: scale,
        parse(value){
            let valueTyped = parseFloat(value);
            let denominator = Math.pow(10, this.scale);
            return Math.round(valueTyped * denominator) / denominator;
        }

    }
};

let Integer = function (){
    return {
        type: 'integer',
        parse(value){
            if (isNaN(value) || 0 === value.length  )  return null; 
           return parseInt(value);
        }

    }
};

let BigInt = function (){
    return {
        type: 'bigint',
        parse(value){
           return parseInt(value);
        }

    }
};

let sqlType = function(type){
    
    switch (type) {
        case String:
        case 'Text':
          return {type: 'Text'};
    
        case Number:
        case 'integer':
        case 'Integer':
     //   case _Integer2.default:
            return Integer();

        case 'BigInt':
            //   case _Integer2.default:
            return BigInt();
      
        case Date:
            return {type:'Date'};
      
        case Boolean:
          return {type:'boolean'};
         
        case (type.match(numeric_re)[ 0 ] === 'numeric' ? type : undefined) : // Type numeric
            let res = type.match(numeric_re);
            return Numeric(parseInt(res[ 2 ].split(',')[ 0 ]), parseInt(res[ 2 ].split(",")[ 1 ]));
    
        default:
          throw new Error('Unknown schema type: ' + (type && type.name || type));
      }
}


var createQuery = function (table_name, columns, drop_yes) {
        let columnsData = [];
        let createQueryStack = [];
        if(Array.isArray(columns)){
            
            columns.map((name, index)=>{
                let columnObj = {};
                columnObj.column_name = name;
                columnObj.column_type = 'text';
                columnObj.primary_key = '';
                columnObj.not_null = '';
                columnsData.push(format(column_type, columnObj));  
            });

            //tableColumns['columns_desc'] = columnsData.join(',');
        }else{
            Object.keys(columns).map((key, index) => {
                let columnObj = {};
                columnObj.column_name = `"${columns[key].prop}"`;
                columnObj.column_type = sqlType(columns[key].sqlType || columns[key].type ).type;
                columnObj.primary_key = '';
                columnObj.not_null = '';
  
                if(index == 0) 
                    columnsData.push('\n'+format(column_type, columnObj)); 
                else 
                    columnsData.push(format(column_type, columnObj));  
            })

        }
        let table_fname = [];
        table_fname.push(table_name.schema);
        table_fname.push(`"${table_name.table}"`);
        if(drop_yes)
            createQueryStack.push(format(queryTemplate1, {table_name:  table_fname.join('.')}));
        createQueryStack.push( format(queryTemplate2, {table_name:  table_fname.join('.'), columns_desc: columnsData.join(',\n')} ));
        return createQueryStack.join(";\n");
}

const exp = {
    createQuery, sqlType
  }

module.exports = exp;
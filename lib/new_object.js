'use strict';
const sql = require('./create_table')


const objHelper = {
    /*

        obj_desc - like 
        1. array = ['prop_name1', 'prop_name2', ..], or 
        2. object:
            {
                prop1: 'prop_name1',
                prop2: 'prop_name2',
                ..
            } 
    */
    createEmptyObj: function (obj_desc) {
        let _obj = {};

        if (Array.isArray(obj_desc)) {
            obj_desc.map((value, index) => {
                _obj[value] = null;
            })
        } else {
            Object.keys(obj_desc).map((value, index) => {
                _obj[`${obj_desc[value].prop}`] = null;
            })
        }

        return _obj;
    },

    initObjDesc: function (obj_desc) {
        Object.keys(obj_desc).map((key, index) => {
            if (!obj_desc[key]['parse'] && !obj_desc[key]['type'] && obj_desc[key]['sqlType']) { // If no type defined, so inhereid it from sqlType like parse function
                let sqltype = sql.sqlType(obj_desc[key].sqlType);
                obj_desc[key]['parse'] = sqltype['parse'].bind(sqltype);
            }
        })
    },

    getColumns: function(obj_desc){
        let columns = []
        Object.keys(obj_desc).map((key, index) => {
            columns.push(`${obj_desc[key].prop}`);
        })
        return columns;
    },

    getValue: function(obj_desc, prop){
        for (const [index, key] of  Object.keys(obj_desc).entries()) {
            if (obj_desc[key].prop === prop){
                return obj_desc[key].value || null;
            }
          }
    },


    setValues: function (obj, values) {
        Object.keys(obj).map((value, index) => {
            obj[value] = values[index];
        })
    },


    getIfHas: function (obj, prop) {
        const result = { valid: true };
        if (prop.indexOf('.') === -1) {
            result.has = prop in obj;
            result.target = obj;
            if (result.has) {
                result.value = obj[prop];
            }
        } else {
            const names = prop.split('.');
            let missing, target;
            for (let i = 0; i < names.length; i++) {
                const n = names[i];
                if (!n) {
                    result.valid = false;
                    return result;
                }
                if (!missing && hasProperty(obj, n)) {
                    target = obj;
                    obj = obj[n];
                } else {
                    missing = true;
                }
            }
            result.has = !missing;
            if (result.has) {
                result.target = target;
                result.value = obj;
            }
        }
        return result;
    }
};

const exp = {
    objHelper
}

Object.freeze(exp);

module.exports = exp;
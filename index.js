'use strict';


if((!process.argv[2])){
    process.exit(1);
}

const CSVARG = process.argv[2];




// @TODO trim blank line

// @TODO CSVPATH from argument
// @TODO .csv ext check


class csv2sqlite3 {
    static init(csvfpath){
        return new Promise((res,rej)=>{
            let path = require('path');
            this.CHAR_CODE = 'utf-8';
            this.CSVPATH =csvfpath;
            this.BASENAME =  path.parse(this.CSVPATH).name;
            this.DBFILE = this.BASENAME+'.db';
            this.TBL = this.BASENAME;
            this.DELIMITER = ',';
            this.EOL = '\r\n';
            this.isTitleLine = true;// first line have column names

            this.pk = 'id';// pkfilename
            this.columns = null;

            this.csvParser = require('csv-parse')({ delimiter: this.DELIMITER });
            this._attachParser();
            // console.log(this);
            
            this.fs = require('fs');
            this.fs.access(this.DBFILE, (err)=>{
                try{
                    if(!err){
                        // chk dbfile exists then delete file
                        this.fs.unlinkSync(this.DBFILE);
                    }
                    this.db =  new (require('sqlite3').verbose().Database)(this.DBFILE);// sqlite3 obj
                    res();
                }catch(err){
                    rej(err);
                }
            });

        });
    }
    static convert(){
        this._openrs()
        .then((stream)=>{
            stream.pipe(csv2sqlite3.csvParser);
        })
        .catch((err)=>{
            console.error(err);
        });

    }
    static _attachParser(){
        this.csvParser.on('data', (data)=>this._csvOnData(data));
        this.csvParser.on('error', (err)=>this._csvOnError(err));
        this.csvParser.on('end', ()=>this._csvFinalize());
    }
    static _openrs(){
        return new Promise((res,rej)=>{
            // chk csvfile exists
            this.fs.access(this.CSVPATH, this.fs.constants.R_OK,
                (err)=>{
                    if(err){
                        rej(err);
                        return false;
                    }
                    res(this.fs.createReadStream(this.CSVPATH));
                }

            );
        });

    }
    static _createTbl(tbl,columns){

        this.db.serialize(()=>{
            this.db.run(`create table ${tbl}( ${columns.join(',')} )`);
        });
    }
    // @TODO use transaction
    static _insDb(tbl, row){
        this.db.serialize(()=>{
            // @TODO use prepared statement
            this.db.run(`insert into ${tbl} values( '${row.join("','")}' )`);
        });       
    }
    static _csvOnData(data){
        if(data === null){
            return false;
        }
        if(this.isTitleLine && this.columns === null){
            this.columns = data;
            this._createTbl(this.TBL, this.columns);
            return true;
        }

        this._insDb(this.TBL, data);
    }
    static _csvOnError(err){
        console.error(err);
        throw err;
    }
    static _csvFinalize(){
       this.db.serialize(()=>{
           this.columns.forEach((v)=>{
               this.db.run(`create index ${v} on ${this.TBL}(${v})`);
           });
       }); 
    }
}

csv2sqlite3.init(CSVARG)
.then(()=>{
    csv2sqlite3.convert();
}).catch((err)=>{
    console.error(err);
    process.exit(1);
});

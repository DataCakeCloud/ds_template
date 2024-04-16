package com.ushareit.data.template.dbetl

import com.fasterxml.jackson.annotation.JsonProperty

case class DBEtlConfig(connectionUrl: String,
                       dbUser: String,
                       dbPassword: String,
                       sourceTable: String,
                       targetTable: String,
                       sql: String,
                       cluster:String,
                       dbType: String,
                       location: String,
                       columns: List[Column],
                       addColumns: List[Column],
                       orderByColumns: List[Column],
                       partitions: String,
                       existTargetTable:Boolean,
                       primaryKey:String
                      )

case class Column(columnName: String, // 目标表字段名
                  columnType: String, // 目标表字段类型
                  comment: String, // 目标表字段注释
                  value: String,
                  funcs: List[Function], // 字段转换函数集合
                  name: String, // 来源表字段名
                  dataType: String // 来源表字段类型
                 )

case class Function(funcName:String,
                    funcParams:List[String])

case class EmailConfig(from:String,
                       to:String,
                       subject:String,
                       sql:String,
                       text:String,
                       from_password:String,
                       format:String,
                       targetType:String)

case class DBListConfig(dbEtlConfig:DBEtlConfig,
                        partition:(String, String),
                        dbTable:String,
                        ddl:String,
                        query:String,
                        tempTable:String
                      )

import 'dart:io';

typedef Json = Map<String, dynamic>;

class Oracle {
  final String username;
  final String password;
  final String ipAddress;
  final int port;
  final String serviceName;

  Oracle({
    required this.username,
    required this.password,
    required this.ipAddress,
    this.port = 1521,
    required this.serviceName,
  });

  String _createFileRecursively(String filename, data) {
    Directory dir = Directory.systemTemp.createTempSync();
    File fileTemp = File('${dir.path}/$filename.sql');
    fileTemp.writeAsString(data);
    fileTemp.createSync();
    return '${dir.path}/$filename';
  }

  List<dynamic> _objToArrayWithComparisionOfAny(Json data, {String? arrow}) {
    String caracter = "=";
    if (arrow != null) caracter = arrow;
    List<dynamic> obj = [];
    List objDataKeys = data.keys.toList();
    List objDataValues = data.values.toList();
    int idx = 0;
    for (var ky in objDataKeys) {
      obj.add('$ky $caracter \'${objDataValues[idx]}\'');
      idx++;
    }
    return obj;
  }

  String _tnsConnect() {
    return "$username/$password@(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=$ipAddress)(PORT=$port)))(CONNECT_DATA=(SERVICE_NAME=$serviceName)))";
  }

  Future<List<dynamic>> _exec(String command) async {
    final ProcessResult shellResponse =
        await Process.run('bash', ['-c', command], runInShell: true);
    if (shellResponse.stdout != null) {
      return shellResponse.stdout
          .toString()
          .replaceAll(RegExp('\t'), '')
          .split("\n")
          .where((element) => element != '')
          .toList();
    }
    return [];
  }

  Json? _checkIrregularity(List? output) {
    if (output!.isEmpty) return {"status": 404, "data": [], "error": 'Not data found'};
    for (String i in output) {
      if (output.isEmpty) return {"status": 404, "data": [], "error": 'Not data found'};
      if (i.contains('ORA-')) return {"status": 500, "data": [], "error": i.toString()};
      if (i.contains('0 rows deleted')) return {"status": 404, "data": [], "error": i.toString()};
      if (i.contains('no rows selected')) return {"status": 404, "data": [], "error": i.toString()};
    }
  }

  Future<List> _fetchColumnsTable(table) async {
    String query =
        'select lower(COLUMN_NAME) as COLUMN_NAME from user_tab_columns where table_name = upper(\'' +
            table +
            '\');';
    final Json descTable = await _sqlplus(query);
    List<dynamic> arry = [];
    for (String col in descTable["data"]) {
      if (!col.contains('rows selected')) {
        arry.add(col);
      }
    }
    return arry;
  }

  Future<Json> _sqlplus(sql, {longchunksize = 3000, long = 35000, lines = 32767}) async {
    print(sql);
    try {
      final String tns = _tnsConnect();
      List data = await _exec(
        'export NLS_LANG=AMERICAN_AMERICA.UTF8 \n sqlplus -s "$tns" <<EOF \n set long $long \n set longchunksize $longchunksize \n set pages 0 \n set lines $lines \n $sql \nEOF',
      );
      Json? existIrregularity = _checkIrregularity(data);
      if (existIrregularity != null) {
        return existIrregularity;
      }
      return {"status": 200, "data": data, "error": null};
    } catch (e) {
      return {"status": 500, "data": "", "error": "Internal Server Error"};
    }
  }

  Future<Json> keepAliveDb() async {
    final Json res = await _sqlplus('select 1 from dual;');
    if (res["status"] == 500) return {"status": 0, "output": "Failed to connect"};
    return {"status": 1, "output": "Successfully connected"};
  }

  Future<Json> insert({table, List? data}) async {
    String valuesData = "";

    if (data != null && data.length > 1) {
      List selects = [];
      List selectsResolved = [];
      for (Map i in data) {
        String values = "";
        int ctx = 0;
        for (var vlr in i.keys.toList()) {
          values = "$values '$vlr' as ${i.keys.toList()[ctx]},";
          ctx += 1;
        }
        selects.add('select $values from dual union all'.replaceAll(', from', ' from'));
      }

      for (int index = 0; index < selects.length; index++) {
        if (index == selects.length - 1) {
          selectsResolved.add(selects[index].replaceAll('union all', ''));
        } else {
          selectsResolved.add(selects[index]);
        }
      }

      String selectsAll = "";
      for (int index = 0; index < selectsResolved.length; index++) {
        if (index == selects.length - 1) {
          selectsAll = '$selectsAll ${selectsResolved[index]}';
        } else {
          selectsAll = '$selectsAll ${selectsResolved[index]}\n';
        }
      }

      String sql = """
ALTER SESSION FORCE PARALLEL DML PARALLEL 5;
commit;
insert /*+ NOAPPEND PARALLEL */
into $table(${data[0].keys.toString().replaceAll("(", '').replaceAll(")", '')})
select * from (
  $selectsAll
);
commit;
ALTER SESSION DISABLE PARALLEL DML;
""";

      String file = _createFileRecursively('sql-insert', sql);
      final Json res = await _sqlplus('@"$file"');
      if (res["status"] == 200) {
        return {
          "status": 200,
          "data": ["Commit complete"],
          "error": null
        };
      }
      return res;
    } else {
      for (String value in data![0].values.toList()) {
        valuesData = valuesData + ',' + "'$value'";
      }

      final String query =
          'insert into $table ( ${data[0].keys.toString().replaceAll("(", '').replaceAll(")", '')} ) values ( ${valuesData.toString().replaceFirst(",", "")} );';
      final Json res = await _sqlplus(query);
      return res;
    }
  }

  Future<Json> insertSelect(
      {tablePrimary, columnsPrimary, tableSource, columnsSource, where, handsFreeWhere}) async {
    String isExistWhere = ";";
    List objWhere = [];

    if (where != null) {
      objWhere = _objToArrayWithComparisionOfAny(where);
      isExistWhere = " where " + objWhere.join(',').replaceAll(',', ' and ') + ";";
    } else if (handsFreeWhere != null) {
      isExistWhere = " where " + handsFreeWhere + ";";
    }

    final Json res = await _sqlplus(
        'insert into $tablePrimary ( $columnsPrimary ) select $columnsSource from $tableSource $isExistWhere');
    return res;
  }

  Future<Json> select({table, List? columns, where, handsFreeWhere}) async {
    String isExistWhere = ";";
    List objWhere = [];

    if (columns == null || columns.length == 1 && columns[0] == '*') {
      columns = await _fetchColumnsTable(table);
    }

    List col = columns.map((arry) {
      if (columns!.indexOf(arry) != columns.length - 1) return arry + "||'|'||";
      return arry;
    }).toList();

    String colString = col.join(',').replaceAll(',', '').trim();

    if (where != null) {
      objWhere = _objToArrayWithComparisionOfAny(where);
      isExistWhere = " where " + objWhere.join(',').replaceAll(',', ' and ') + ";";
    } else if (handsFreeWhere != null) {
      isExistWhere = " where " + handsFreeWhere + ";";
    }
    String query = 'select ${colString.replaceAll("[ ]", ",")} from $table $isExistWhere';

    Json res = await _sqlplus(query);

    List obj = [];
    if (res["data"].length > 0 && res["error"] == null) {
      for (String v in res["data"]) {
        Json newObj = {};
        List data = v.split("|");
        for (int index = 0; index < data.length; index++) {
          if (!data[index].toString().contains('rows selected')) {
            newObj[columns[index]] = data[index].toString().trim();
          }
        }
        if (newObj.keys.toList().isNotEmpty) obj.add(newObj);
      }
      res["data"] = obj;
    }
    return res;
  }

  Future<Json> update({table, data, updateAll, where, handsFreeWhere}) async {
    String isExistWhere = ";";
    List objUpdate = _objToArrayWithComparisionOfAny(data);
    List objWhere = [];
    if (updateAll != null) {
      isExistWhere = ";";
    } else if (where != null) {
      objWhere = _objToArrayWithComparisionOfAny(where);
      isExistWhere = " where " + objWhere.join(',').replaceAll(',', ' and ') + ";";
    } else if (handsFreeWhere != null) {
      isExistWhere = " where " + handsFreeWhere + ";";
    }
    final Json res = await _sqlplus('update $table set $objUpdate $isExistWhere');
    return res;
  }

  Future<Json> delete({table, deleteAll, where, handsFreeWhere}) async {
    String isExistWhere = ";";
    List objWhere = [];
    if (deleteAll != null) {
      isExistWhere = ";";
    } else if (where != null) {
      objWhere = _objToArrayWithComparisionOfAny(where);
      isExistWhere = " where " + objWhere.join(',').replaceAll(',', ' and ') + ";";
    } else if (handsFreeWhere != null) {
      isExistWhere = " where " + handsFreeWhere + ";";
    }
    final Json res = await _sqlplus('delete ' + table + isExistWhere);
    return res;
  }

  Future<Json> execProcedure({procedureName, Json? data}) async {
    String value = '';
    if (data != null) {
      value = _objToArrayWithComparisionOfAny(data, arrow: '=>')
          .toString()
          .replaceAll("[", "")
          .replaceAll("]", "");
      value = '($value)';
    }

    final String query = 'begin \n $procedureName$value;\n end; \n/';
    Json res = await _sqlplus(query);
    return res;
  }

  Future<Json> execFunction({functionName, Json? data}) async {
    String value = '';
    if (data != null) {
      value = _objToArrayWithComparisionOfAny(data, arrow: '=>')
          .toString()
          .replaceAll("[", "")
          .replaceAll("]", "");
      value = '($value)';
    }

    final String query = 'select $functionName$value as response from dual;';
    Json res = await _sqlplus(query);
    return res;
  }

  Future<Json> truncate({table}) async {
    String dml = 'TRUNCATE TABLE $table;';
    final Json res = await _sqlplus(dml);
    return res;
  }
}

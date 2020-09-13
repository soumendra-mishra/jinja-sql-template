import yaml
from jinja2 import Environment, FileSystemLoader
from google.cloud import bigquery

def getTableColumns(baseTables):
    client = bigquery.Client()
    columnList = []
    for tables in baseTables:
        datasetName = tables.split(".")[0]
        tableName = tables.split(".")[1]
        tableRef = client.dataset(datasetName).table(tableName)
        table = client.get_table(tableRef)
        for column in table.schema:
            if column.name not in str(columnList):
                columnList.append(tableName + "." + column.name)
    return columnList


def getViewColumns(columnList, excludeColumns):
    viewColumns = set(columnList).difference(set(excludeColumns))
    viewColumnList = sorted(list(viewColumns))
    return viewColumnList


def getTableJoin(baseTables):
    columnList = []
    for tables in baseTables[1:]:
        tableName = tables.split(".")[1]
        columnList.append(tableName)

    return baseTables[0].split(".")[1], columnList


def runSql(sqlQuery):
    client = bigquery.Client()
    sqlJob = client.query(sqlQuery)
    sqlJob.result()


configList = yaml.safe_load(open("config.yaml"))
env = Environment(loader=FileSystemLoader("./"))
sqlTemplate = env.get_template('sql_template.sql')

for tableConf in configList:
    viewName = tableConf['viewName']
    baseTables = tableConf['baseTables']
    joinKey = tableConf['joinKey']
    filterConditions = tableConf['filterConditions']
    excludeColumns = tableConf['excludeColumns']

    tableColumnList = getTableColumns(baseTables)
    viewColumnList = getViewColumns(tableColumnList, excludeColumns)

    params = {
        'viewName': viewName,
        'baseTables': baseTables,
        'viewColumnList': viewColumnList,
        'filterConditions': filterConditions,
        'tableCount': len(baseTables)
    }

    if len(baseTables) > 1:
        tableL, tableR = getTableJoin(baseTables)
        params['tableL'] = tableL
        params['tableR'] = tableR
        params['joinKey'] = joinKey

    sqlQuery = sqlTemplate.render(params=params)
    runSql(sqlQuery)

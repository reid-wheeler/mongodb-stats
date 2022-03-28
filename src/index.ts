import { Collection, CollStats, Document, ListDatabasesResult, MongoClient } from "mongodb";

// Connection URL
const url = 'mongodb://prod-r:prod-reader-rta@localhost:27020';
const client = new MongoClient(url, { directConnection: true });

type DbInfo = {
    name: string;
    size: number | undefined;
    collections: CollectionInfo[];
};

type CollectionInfo = {
    name: string;
    size: number;
    indexes: IndexInfo[];
};

type IndexInfo = {
    name: string;
    size: number;
    accesses: number
};

async function main() {
    await client.connect();

    let listDatabasesResult: ListDatabasesResult = await client.db().admin().listDatabases();
    for(const db of listDatabasesResult.databases) {
        if(db.name !== "config" && db.name !== "admin") {
            let dbInfoItem: DbInfo = await getDbInfoItem(db);
            printReport(dbInfoItem);
        }

    }
}


async function getDbInfoItem(db: any): Promise<DbInfo>  {
    let collections: Collection<Document>[] = await client.db(db.name).collections();
    let collectionInfoList: CollectionInfo[] = await (await Promise.all(collections.map(getCollectionInfoItem))).filter(x => x !== undefined);
    collectionInfoList.sort((a,b) => (a.size < b.size) ? 1 : ((b.size < a.size) ? -1 : 0));
    return new Promise((resolve) => {
        const dbInfo: DbInfo = { 
            name: db.name,
            size: db.sizeOnDisk,
            collections: collectionInfoList
        }
        resolve(dbInfo);
    });
}

async function getCollectionInfoItem(collection: Collection<Document>): Promise<CollectionInfo> {
    try {
        let collStats: CollStats = await collection.stats();
        let indexStats: Document[] = await collection.aggregate([{$indexStats:{}}]).toArray();
        let usageMap = new Map();
        for(const indexStat of indexStats) {
            usageMap.set(indexStat.name, indexStat.accesses.ops)
        }
        let indexInfoList: IndexInfo[] = [];
        for(const indexName of Object.keys(collStats.indexSizes)) {
            const indexInfo: IndexInfo = {
                name: indexName,
                size: collStats.indexSizes[indexName],
                accesses: usageMap.get(indexName)
            };
            indexInfoList.push(indexInfo);
        };
        indexInfoList.sort((a,b) => (a.size < b.size) ? 1 : ((b.size < a.size) ? -1 : 0));
        return new Promise((resolve) => {
            resolve({
                name: collection.collectionName,
                size: collStats.size,
                indexes: indexInfoList
            });
        });
    } catch(error: any) {
        return new Promise((resolve) => {
            resolve(undefined);
        });
    }
}

function printReport(dbInfo: DbInfo) {
    console.log("DB Name: " + dbInfo.name + ", size: " + dbInfo.size);
    if(dbInfo.collections.length > 0) {
        console.log("   " + dbInfo.name + " collections: ");
    } else {
        console.log("   No collections information available for DB: " + dbInfo.name);
    }
    for(const collection of dbInfo.collections) {
        if(collection.name.length > 0 && collection.size > 0) {
            console.log("       collection name: " + collection.name + ", size: " + collection.size);
            for(const index of collection.indexes) {
                console.log("           index name: " + index.name + ", size: " + index.size + ", accesses: " +index.accesses);
            }
            console.log("\n");
        }
    }
    console.log("\n---------------------------------------------------------------------------------------------------\n");
}

main()
  .catch(console.error)
  .finally(() => client.close());
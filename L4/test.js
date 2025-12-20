const { MongoClient } = require('mongodb');

const url = "mongodb://mongo1:27017,mongo2:27017,mongo3:27017/?replicaSet=myReplicaSet&serverSelectionTimeoutMS=5000";
const dbName = 'perf_test';

async function runTest(wcLabel, wcValue) {
    const client = new MongoClient(url);
    try {
        await client.connect();
        const db = client.db(dbName);
        const col = db.collection('likes_counter');

        await col.drop().catch(() => {});
        await col.insertOne({ _id: 'global_counter', points: 0 });

        console.log(`--- Старт тесту: WriteConcern = ${wcLabel} ---`);
        const start = Date.now();

        const worker = async (id) => {
            const workerClient = new MongoClient(url);
            await workerClient.connect();
            const workerCol = workerClient.db(dbName).collection('likes_counter', { writeConcern: { w: wcValue } });
            
            for (let i = 0; i < 10000; i++) {
                try {
                    await workerCol.findOneAndUpdate(
                        { _id: 'global_counter' },
                        { $inc: { points: 1 } }
                    );
                } catch (e) {
                    i--; 
                    await new Promise(r => setTimeout(r, 500));
                }
            }
            await workerClient.close();
        };

        console.log(">>> <<<");
        await Promise.all(Array.from({ length: 10 }, (_, i) => worker(i)));

        const finalDoc = await col.findOne({ _id: 'global_counter' });
        console.log(`Результат: ${finalDoc.points}/100000`);
        console.log(`Час виконання: ${(Date.now() - start) / 1000} сек`);

    } finally {
        await client.close();
    }
}

// runTest('1', 1);
runTest('majority', 'majority');
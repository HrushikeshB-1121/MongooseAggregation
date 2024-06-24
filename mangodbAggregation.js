const { MongoClient } = require('mongodb');

async function transformData() {
  const uri = 'mongodb://localhost:27017';
  const client = new MongoClient(uri);
  await client.connect();
  const database = client.db('covidDetails');

  try {


    const data = database.collection('covidIndia');
    
    /* Displays no of deaths in each state */
    const pipeline = [
        {
          $group:
            {
              _id: "$State/UnionTerritory",
              totalDeaths: {
                $sum: "$Deaths"
              }
            }
        }
      ];

    const results = await data.aggregate(pipeline).toArray();
    console.log("Displays no of deaths in each state",results);


    /* top 5 states */
    const pipeline1 = [
        {
          $group:
            {
              _id: "$State/UnionTerritory",
              totalDeaths: {
                $sum: "$Deaths"
              }
            }
        },
        {
          $sort: {
            totalDeaths: -1
          }
        },{
          $limit:5
        }
      ];

    const resultsone = await data.aggregate(pipeline1).toArray();
    console.log(" top 5 states",resultsone);


      /* count no in a state */
    const pipeline2 = [
        {
            $match:
              {
                "State/UnionTerritory": "Karnataka"
              }
        },
        {
          $group:
            {
              _id: "$State/UnionTerritory",
              totalDeaths: {
                $sum: "$Deaths"
              }
            }
        }
      ];

    const resultstwo = await data.aggregate(pipeline2).toArray();
    console.log("count no in a state",resultstwo);


    const statedata = database.collection('covidStateWise');

    
    const pipeline3 = [
      {
        $group:{
          _id: "$State",
          totalDoses: {$sum: "$Total Doses Administered"},
          totalFirstDose: {$sum: "$First Dose Administered"},
          totalSecondDose: {$sum: "$Second Dose Administered"}
        }
      }
    ];
    
    const results3 = await statedata.aggregate(pipeline3).toArray();
    console.log(results3);
    
    const pipeline4 = [
      {
        $group:
        {
          _id: "$State",
          totalDoses: {$sum: "$Total Doses Administered"},
          totalFirstDose: {$sum: "$First Dose Administered"},
          totalSecondDose: {$sum: "$Second Dose Administered"}
        }
      },
      {
        $match:{
          _id:"India"
        }
      }
    ];

  const results4 = await statedata.aggregate(pipeline4).toArray();
  console.log(results4);

  } finally {
    await client.close();
  }
}

transformData().catch(console.error);
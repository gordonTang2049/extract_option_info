const main = async () => {

    const info = process.argv
    const codeShare = info[2]
    const assetClass = info[3]

  
    const URL = `https://api.nasdaq.com/api/quote/${codeShare}/option-chain?assetclass=${assetClass}&limit=9999&fromdate=all&todate=undefined&excode=oprac&callput=callput&money=all&type=all`;
    const response = await fetch(URL);
    const jsonData = await response.json();

    process.stdout.write(JSON.stringify(jsonData.data, null, 2))
  }

main()


const { StringStream } = require("scramjet");
const request = require("request");

const csvFile = "https://drive.google.com/uc?authuser=0&id=1ggTN3ayPcAdl9jvrggb54-WZU3IKOaOP&export=download";

let firstRow = true;
let accounts = {};
let uniqueBuySellCombinations = new Set();

let answerOne = 0;
let answerTwo = 0;
let answerThree = [];

/**
 * Generates the data to be processed
 * @param {Array} row 
 */
const consumeRow = (row) => {
    if (!firstRow) {
        // gets only unique buy/sell combinations
        uniqueBuySellCombinations.add(row[0] + row[1]);

        // gets transactions grouped by accounts
        let account = accounts[row[3]];
        if (!account)
            account = {
                transactions: [],
                sum: 0
            };
        account.transactions.push(row);
        account.sum += parseFloat(row[2]);
        accounts[row[3]] = account;
    } else
        firstRow = false;
}

/**
 * Process data to answer the questions
 */
const processTransactions = () => {
    // answerOne: gets the size of uniqueBuySellCombinations set
    answerOne = uniqueBuySellCombinations.size;

    // all averages to answerThree
    let averages = new Map();

    // process transactions
    Object.entries(accounts).forEach(([accountId, account]) => {
        // answerTwo: only accounts executed at least 500 transactions
        if (account.transactions.length >= 500)
            answerTwo++;
        // generates averages
        average = account.sum / account.transactions.length;
        averages.set(average, accountId)
    });

    // answerThree: gets the 3rd highest average transaction amount
    let averagesSorted = new Map([...averages.entries()].sort());
    let averagesSortedValues = Array.from(averagesSorted.values());
    answerThree = averagesSortedValues[averagesSortedValues.length - 3];
}

const answerQuestions = () => {
    // gets the answers
    processTransactions();

    console.log('1) How many unique buy/sell combinations are there?');
    console.log('   Answer: ', answerOne)
    console.log('2) How many accounts executed at least 500 transactions?');
    console.log('   Answer: ', answerTwo)
    console.log('3) Which account has the 3rd highest average transaction amount?');
    console.log('   Answer: ' + answerThree)
}

console.log('Reading CSV file from web...');
request.get(csvFile)            // fetch csv
    .pipe(new StringStream())   // pass to stream
    .CSVParse()                 // parse into objects
    .consume(consumeRow)
    .then(answerQuestions)
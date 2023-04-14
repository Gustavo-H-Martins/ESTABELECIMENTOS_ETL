// app/swagger.js (Gerador de documentação da Api)
/**
* Libs
*/
const swaggerAutogen = require('swagger-autogen')();

const outputFile = './swagger/swagger.json';
const endpointsFiles = ['./app.js'];

swaggerAutogen(outputFile, endpointsFiles);
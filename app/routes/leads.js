//app/routes/leads.js
/**
 * Libs
 */
const db = require('../config/db');
const logToDatabase = require('../config/log')
const router = require('express').Router();

/**
 * Funções de apoio
 */
// INCLUSÃO DA FUNÇÃO DE VALIDAÇÃO DE ENTRADA DE BANDEIRAS
function generateLikeClause(columnName, values, conditions) {
  const likeClauses = values.map((value) => `${columnName} LIKE "%${value}%" AND ${conditions}`);
  return likeClauses.join(' OR ');
}
// INCLUSÃO DA FUNÇÃO QUE RECEBE O ARRAY E OS PARÂMETROS DO GROUPBY E CONCATENA
// Usando reduce para criar um objeto que soma os valores por grupo
function agrupar(array, groupby) {
  // Criando um objeto intermediário que acumula os valores por grupo
  let acumulador = array.reduce((objeto, atual) => {
    // Verificando se o groupby contém a propriedade BANDEIRAS
    if (groupby.includes("BANDEIRAS")) {
      // Convertendo a string BANDEIRAS em um array de strings
      let bandeiras = atual.BANDEIRAS.split(',').map(element => element.trim());
      
      // Iterando sobre cada elemento do array de bandeiras
      bandeiras.forEach(bandeira => {
        // Criando uma chave composta pelas propriedades do groupby, substituindo a propriedade BANDEIRAS pelo valor da bandeira atual
        let chave = groupby.map(prop => prop === "BANDEIRAS" ? bandeira : atual[prop]).join("|");
        // Se a chave não existir no objeto, iniciando com um objeto com as mesmas propriedades do atual, mas com TOTAL zero
        if (!objeto[chave]) {
          objeto[chave] = { ...atual, TOTAL: 0 };
        }
        // Somando o valor ao objeto
        objeto[chave].TOTAL += atual.TOTAL;
      });
    } //se groupby não tiver a propriedade BANDEIRAS 
    else {
      // Criando uma chave composta pelas propriedades do groupby
      let chave = groupby.map(prop => atual[prop]).join("|");
      // Se a chave não existir no objeto, iniciando com um objeto com as mesmas propriedades do atual, mas com TOTAL zero
      if (!objeto[chave]) {
        objeto[chave] = { ...atual, TOTAL: 0 };
      }
      // Somando o valor ao objeto
      objeto[chave].TOTAL += atual.TOTAL;
    }
    // Retornando o objeto para a próxima iteração
    return objeto;
  }, {}); // O objeto inicial é vazio

  // Convertendo o objeto intermediário em um array de pares [chave, valor]
  let pares = Object.entries(acumulador);

  // Transformando cada par em um objeto com as propriedades desejadas
  let resultado = pares.map(par => {
     // Separando a chave em um array de valores
     let valores = par[0].split("|");
     // Criando um objeto com as propriedades do groupby e seus respectivos valores
     let grupo = groupby.reduce((obj, prop, i) => {
       obj[prop] = valores[i];
       return obj;
     }, {});
     // Adicionando a propriedade TOTAL ao objeto
     grupo.TOTAL = par[1].TOTAL;
     // Retornando o objeto para o array final
     return grupo;
  });

  // Retornando o array final
  return resultado;
}
/**
 * Router - estrutura
 */
// GetAll
router.route("/estabelecimentos")
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
    const associados = req.query.associados ? req.query.associados.split(",") : null;
    const associadosTuple = associados ? `(${associados.map((valor) => `"${valor.toUpperCase()}"`).join(",")})` : null;
    const souabrasel = req.query.souabrasel ? req.query.souabrasel.split(",") : null;
    const souabraselTuple = souabrasel ? `(${souabrasel.map((valor) => `"${valor.toUpperCase()}"`).join(",")})` : null;
    //const bandeira = req.query.bandeira ? [req.query.bandeira.toLowerCase()] : null;
    // INCLUSÃO DA VALIDAÇÃO DE ENTRADA DE BANDEIRAS
    const bandeira = req.query.bandeira ? req.query.bandeira.split(",") : null;
    const bandeiraTuple = bandeira ? bandeira.map((valor) => valor.toUpperCase().replace(/-/g, ' ')) : null;

    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairros = req.query.bairro ? req.query.bairro.split(",") : null;
    const bairroTuple = bairros ? `(${bairros.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;
    const page = parseInt(req.query.page) || 1;
    const pageSize = parseInt(req.query.pageSize) || 100;
    const offset = (page - 1) * pageSize;
    const raio = parseFloat(req.query.raio) || 10;
    const lat = parseFloat(req.query.lat) || 0
    const lon = parseFloat(req.query.lon) || 0
    let query = `
        SELECT DISTINCT * FROM RECEITA
        --
        /**/
        `;
    let conditions = [];
    if (lat !== 0 && lon !== 0) conditions.push(` (ACOS(SIN(RADIANS('${lat}')) * SIN(RADIANS(LATITUDES)) + COS(RADIANS('${lat}')) * COS(RADIANS(LATITUDES)) * COS(RADIANS(LONGITUDES) - RADIANS('${lon}'))) * 6371) <= ${raio}`);
    if (associados) conditions.push(`ASSOCIADO IN ${associadosTuple}`);
    if (souabrasel) conditions.push(`SOU_ABRASEL IN ${souabraselTuple}`)
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairros) conditions.push(`BAIRRO IN ${bairroTuple}`);
    if (cnpj === null) conditions.push(`CNPJ IS NOT NULL`);
    // Validação dos parâmetros das bandeiras
    if (bandeira.length === 1) query = query.replace('FROM RECEITA', `FROM ${bandeira}`);
    if (bandeiraTuple) likeClause = bandeiraTuple ? generateLikeClause('BANDEIRAS', bandeiraTuple, conditions.join(' AND ')) : null;
    if (bandeira) query = query.replace(/--/g, ` WHERE ${likeClause}`);;
    if (conditions.length > 0 && bandeira === null) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);
    if (associados == 0) query = query.replace(`ASSOCIADO IN ("0")`, `ASSOCIADO = 0`);
    if (souabrasel == 0) query = query.replace(`SOU_ABRASEL IN ("0")`, `SOU_ABRASEL = 0`);
    query += `LIMIT ? OFFSET ?;`;

    //console.log(query)

    db.all(query, [pageSize, offset], (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      // Transforma a string em um array e remove os espaços em branco
      rows = rows.map(row => {
        row.BANDEIRAS = row.BANDEIRAS.split(',').map(element => element.trim());
        return row;
      });
      /*
      // Formatando data e hora para incluir no log
      const date = new Date()
      const formattedDate = `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`; // formata a data como DD/MM/AAAA
      const formattedTime = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`; // formata a hora como HH:MM:SS
      dataChamada = `Data: ${formattedDate} - Hora: ${formattedTime}`
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${bandeira}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)
        */
      res.status(200).json(
        rows
      );
    });
  })
// GetAll-contagem
router.route("/estabelecimentos/counts")
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
    //const bandeira = req.query.bandeira ? [req.query.bandeira.toLowerCase()] : null;
    // INCLUSÃO DA VALIDAÇÃO DE ENTRADA DE BANDEIRAS
    const bandeira = req.query.bandeira ? req.query.bandeira.split(",") : null;
    const bandeiraTuple = bandeira ? bandeira.map((valor) => valor.toUpperCase().replace(/-/g, ' ')) : null;

    const associados = req.query.associados ? req.query.associados.split(",") : null;
    const associadosTuple = associados ? `(${associados.map((valor) => `"${valor.toUpperCase()}"`).join(",")})` : null;
    const souabrasel = req.query.souabrasel ? req.query.souabrasel.split(",") : null;
    const souabraselTuple = souabrasel ? `(${souabrasel.map((valor) => `"${valor.toUpperCase()}"`).join(",")})` : null;
    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairros = req.query.bairro ? req.query.bairro.split(",") : null;
    const bairroTuple = bairros ? `(${bairros.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;
    const groupby = req.query.groupby ? req.query.groupby.toLocaleUpperCase().split(",") : null;
    const orderby = req.query.orderby ? [req.query.orderby.toLocaleUpperCase()] : "DESC";
    let query = `
                  SELECT DISTINCT COUNT(*) AS TOTAL FROM RECEITA
                  --
                  /**/
                  ;
                  `;
    let conditions = [];
    if (associados) conditions.push(`ASSOCIADO IN ${associadosTuple}`);
    if (souabrasel) conditions.push(`SOU_ABRASEL IN ${souabraselTuple}`)
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairros) conditions.push(`BAIRRO IN ${bairroTuple}`);
    if (cnpj === null) conditions.push(`CNPJ IS NOT NULL`);

    // Validação dos parâmetros das bandeiras
    if (bandeira.length === 1) query = query.replace('FROM RECEITA', `FROM ${bandeira}`);
    if (bandeiraTuple) likeClause = bandeiraTuple ? generateLikeClause('BANDEIRAS', bandeiraTuple, conditions.join(' AND ')) : null;
    if (bandeira) query = query.replace(/--/g, ` WHERE ${likeClause}`);;
    if (conditions.length > 0 && bandeira === null) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);

    if (groupby) query = query.replace(/\/\*\*\//g, ` GROUP BY ${groupby}`);
    if (groupby) query = query.replace(/COUNT\(\*\)/g, `${groupby}, COUNT(*)`);
    if (orderby) query = query.replace(";", `ORDER BY TOTAL ${orderby} ;`);
    if (associados == 0) query = query.replace(`ASSOCIADO IN ("0")`, `ASSOCIADO = 0`);
    if (souabrasel == 0) query = query.replace(`SOU_ABRASEL IN ("0")`, `SOU_ABRASEL = 0`);
    //console.log(query)

    db.all(query, (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      // Testando a função com diferentes parâmetros
      rows = agrupar(rows, groupby);
      //console.log(rows)
      /*
      //console.log(query)
      // Formatando data e hora para incluir no log
      const date = new Date()
      const formattedDate = `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`; // formata a data como DD/MM/AAAA
      const formattedTime = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`; // formata a hora como HH:MM:SS
      dataChamada = `Data: ${formattedDate} - Hora: ${formattedTime}`
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${bandeira}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)
      */
      res.status(200).json(
        rows
      );
    });
  })
// GET BAIRROS
router.route('/estabelecimentos/bairros')
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
    //const bandeira = req.query.bandeira ? [req.query.bandeira.toLowerCase()] : null;
    // INCLUSÃO DA VALIDAÇÃO DE ENTRADA DE BANDEIRAS
    const bandeira = req.query.bandeira ? req.query.bandeira.split(",") : null;
    const bandeiraTuple = bandeira ? bandeira.map((valor) => valor.toUpperCase().replace(/-/g, ' ')) : null;

    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairros = req.query.bairro ? req.query.bairro.split(",") : null;
    const bairroTuple = bairros ? `(${bairros.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;

    let query = `SELECT DISTINCT(BAIRRO) FROM RECEITA
                --
                ;`;
    let conditions = [];

    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairros) conditions.push(`BAIRRO IN ${bairroTuple}`);
    if (cnpj === null) conditions.push(`CNPJ IS NOT NULL`);

    // Validação dos parâmetros das bandeiras
    if (bandeira.length === 1) query = query.replace('FROM RECEITA', `FROM ${bandeira}`);
    if (bandeiraTuple) likeClause = bandeiraTuple ? generateLikeClause('BANDEIRAS', bandeiraTuple, conditions.join(' AND ')) : null;
    if (bandeira) query = query.replace(/--/g, ` WHERE ${likeClause}`);;
    if (conditions.length > 0 && bandeira === null) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);

    //console.log(query)
    db.all(query, (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      //console.log(query)
      // Formatando data e hora para incluir no log
      const date = new Date()
      const formattedDate = `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`; // formata a data como DD/MM/AAAA
      const formattedTime = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`; // formata a hora como HH:MM:SS
      dataChamada = `Data: ${formattedDate} - Hora: ${formattedTime}`
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${bandeira}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)
      res.status(200).json(
        rows
      );
    });
  })
// get cidades
router.route('/estabelecimentos/cidades')
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
    //const bandeira = req.query.bandeira ? [req.query.bandeira.toLowerCase()] : null;
    // INCLUSÃO DA VALIDAÇÃO DE ENTRADA DE BANDEIRAS
    const bandeira = req.query.bandeira ? req.query.bandeira.split(",") : null;
    const bandeiraTuple = bandeira ? bandeira.map((valor) => valor.toUpperCase().replace(/-/g, ' ')) : null;

    const uf = req.query.uf ? [req.query.uf.toUpperCase()] : null;
    const cidade = req.query.cidade ? [req.query.cidade.toUpperCase().replace(/-/g, ' ')] : null;
    const bairros = req.query.bairro ? req.query.bairro.split(",") : null;
    const bairroTuple = bairros ? `(${bairros.map((valor) => `"${valor.toUpperCase().replace(/-/g, ' ')}"`).join(",")})` : null;

    let query = `SELECT DISTINCT(CIDADE) FROM RECEITA
                ;`;
    let conditions = [];
    if (uf) conditions.push(`UF = "${uf}"`);
    if (cidade) conditions.push(`CIDADE = "${cidade}"`);
    if (bairros) conditions.push(`BAIRRO IN ${bairroTuple}`);
    if (cnpj === null) conditions.push(`CNPJ IS NOT NULL`);

    // Validação dos parâmetros das bandeiras
    if (bandeira.length === 1) query = query.replace('FROM RECEITA', `FROM ${bandeira}`);
    if (bandeiraTuple) likeClause = bandeiraTuple ? generateLikeClause('BANDEIRAS', bandeiraTuple, conditions.join(' AND ')) : null;
    if (bandeira) query = query.replace(/--/g, ` WHERE ${likeClause}`);;
    if (conditions.length > 0 && bandeira === null) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);


    db.all(query, (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      //console.log(query)
      // Formatando data e hora para incluir no log
      const date = new Date()
      const formattedDate = `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`; // formata a data como DD/MM/AAAA
      const formattedTime = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`; // formata a hora como HH:MM:SS
      dataChamada = `Data: ${formattedDate} - Hora: ${formattedTime}`
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${bandeira}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)
      res.status(200).json(
        rows
      );
    });
  })

module.exports = router;
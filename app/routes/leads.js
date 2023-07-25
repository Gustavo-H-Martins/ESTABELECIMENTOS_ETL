//app/routes/leads.js
/**
 * Libs
 */
const db = require('../config/db');
const logToDatabase = require('../config/log')
const router = require('express').Router();
const ExcelJS = require('exceljs');
const {Parser} = require('json2csv');
const { salvaCache, lerCache } = require('./cache');

/**
 * Funções de apoio
 */
// Função para formatar o valor do CNPJ
const formatCnpj = (cnpj) => {
  // Remover caracteres não numéricos
  cnpj = cnpj.replace(/\D/g, '');

  // Adicionar os separadores
  cnpj = cnpj.replace(/(\d{2})(\d)/, '$1.$2');
  cnpj = cnpj.replace(/(\d{3})(\d)/, '$1.$2');
  cnpj = cnpj.replace(/(\d{3})(\d)/, '$1/$2');
  cnpj = cnpj.replace(/(\d{4})(\d)/, '$1-$2');

  return cnpj;
};

// INCLUSÃO DA FUNÇÃO PARA CONVERTER JSON EM CSV
const jsonToCsv = (data, cabecalho) => {
  // Declarar variável fields
  let fields;

  if (cabecalho) {
    // Definir o cabeçalho
    fields = cabecalho;
  } else {
    // Obter as chaves do primeiro objeto como colunas do CSV
    fields = Object.keys(data[0]);
  }

  // Criar um parser com as opções desejadas
  const parser = new Parser({ fields, delimiter: ';' });
  
  // Converter os dados em um array de objetos com os campos desejados
  const dataWithLinks = data.map((obj) => ({
      CNPJ: formatCnpj(obj.CNPJ),
      NOME_FANTASIA: obj.NOME_FANTASIA,
      CIDADE: obj.CIDADE,
      BAIRRO: obj.BAIRRO,
      ENDERECO: obj.ENDERECO,
      TELEFONE: obj.TELEFONE,
      EMAIL: obj.EMAIL,
      ASSOCIADO: obj.ASSOCIADO == 0 ? 'NÃO' : obj.ASSOCIADO,
      SOU_ABRASEL: obj.SOU_ABRASEL == 0 ? 'NÃO' : obj.SOU_ABRASEL,
      LINK_GOOGLE: `https://www.google.com/search?q=${obj.ESTABELECIMENTO}+${obj.UF}+${obj.CIDADE}+${obj.BAIRRO}`,
      LINK_GOOGLE_MAPS: `https://www.google.com/maps/search/${obj.ESTABELECIMENTO}+${obj.UF}+${obj.CIDADE}+${obj.BAIRRO}`
    }));

  // Converter os dados em csv
  const csv = parser.parse(dataWithLinks);

  // Retornar o csv como um buffer
  return Buffer.from(csv);
};

// INCLUSÃO DA FUNÇÃO PARA CONVERTER DADOS JSON EM XLSX
const jsonToXlsx = async (data, cabecalho) => {
  // Criar um novo workbook
  const workbook = new ExcelJS.Workbook();
  
  // Adicionar uma planilha ao workbook
  const worksheet = workbook.addWorksheet('Dados');

  // Declarar a variável dataAsArray
  let dataAsArray;
  
  // Adicionar cabeçalho
  if (cabecalho) {
    // Adicionar uma linha com os títulos das colunas
    const headerRow = worksheet.addRow(cabecalho);

    // Deixar todos os títulos das colunas em negrito
    headerRow.eachCell((cell) => {
      cell.font = { bold: true };
    });
    ["CNPJ", "NOME FANTASIA", "CIDADE", "BAIRRO", "ENDEREÇO", "TELEFONE", "E-MAIL", "ASSOCIADO", "TEM SOU ABRASEL", "LINK GOOGLE", "LINK GOOGLE MAPS"]
    // Converter os dados em um array de arrays
    dataAsArray = data.map((obj) => [
      formatCnpj(obj.CNPJ),
      obj.NOME_FANTASIA,
      obj.CIDADE,
      obj.BAIRRO,
      obj.ENDERECO,
      obj.TELEFONE,
      obj.EMAIL,
      obj.ASSOCIADO == 0 ? 'NÃO' : obj.ASSOCIADO,
      obj.SOU_ABRASEL == 0 ? 'NÃO' : obj.SOU_ABRASEL,
      `https://www.google.com/search?q=${obj.ESTABELECIMENTO}+${obj.UF}+${obj.CIDADE}+${obj.BAIRRO}`,
      `https://www.google.com/maps/search/${obj.ESTABELECIMENTO}+${obj.UF}+${obj.CIDADE}+${obj.BAIRRO}`
    ]);
  }  else {
    // Converter os dados em um array de arrays
    dataAsArray = data.map((obj) => Object.values(obj));
    }
  // Adicionar os dados à planilha
  worksheet.addRows(dataAsArray);

  // Adicionar uma borda ao redor de todas as células da planilha
  worksheet.eachRow((row) => {
    row.eachCell((cell) => {
      cell.border = {
        top: { style: 'thin' },
        left: { style: 'thin' },
        bottom: { style: 'thin' },
        right: { style: 'thin' }
      };
    });
  });
  // Retornar o workbook como um stream
  return workbook.xlsx.writeBuffer();
};


// INCLUSÃO DA FUNÇÃO DE VALIDAÇÃO DE ENTRADA D ORIGEM
function generateLikeClause(columnName, values, conditions) {
  const likeClauses = values.map((value) => `${columnName} LIKE "%${value}%" AND ${conditions}`);
  return likeClauses.join(' OR ');
}
// INCLUSÃO DA FUNÇÃO QUE RECEBE O ARRAY E OS PARÂMETROS DO GROUPBY E CONCATENA
// Usando reduce para criar um objeto que soma os valores por grupo
function agrupar(array, groupby) {
  // Criando um objeto intermediário que acumula os valores por grupo
  let acumulador = array.reduce((objeto, atual) => {
    // Verificando se o groupby contém a propriedade ORIGEM
    if (groupby.includes("ORIGEM")) {
      // Convertendo a string ORIGEM em um array de strings
      let origens = atual.ORIGEM.split(',').map(element => element.trim());

      // Iterando sobre cada elemento do array de origem
      origens.forEach(origem => {
        // Criando uma chave composta pelas propriedades do groupby, substituindo a propriedade ORIGEM pelo valor da origem atual
        let chave = groupby.map(prop => prop === "ORIGEM" ? origem : atual[prop]).join("|");
        // Se a chave não existir no objeto, iniciando com um objeto com as mesmas propriedades do atual, mas com TOTAL zero
        if (!objeto[chave]) {
          objeto[chave] = { ...atual, TOTAL: 0 };
        }
        // Somando o valor ao objeto
        objeto[chave].TOTAL += atual.TOTAL;
      });
    } //se groupby não tiver a propriedade origens 
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
  .get(async function(req, res, next) {
    const clientIp = req.ip;
    const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
    const associados = req.query.associados ? req.query.associados.split(",") : null;
    const associadosTuple = associados ? `(${associados.map((valor) => `"${valor.toUpperCase()}"`).join(",")})` : null;
    const souabrasel = req.query.souabrasel ? req.query.souabrasel.split(",") : null;
    const souabraselTuple = souabrasel ? `(${souabrasel.map((valor) => `"${valor.toUpperCase()}"`).join(",")})` : null;
    //const bandeira = req.query.bandeira ? [req.query.bandeira.toLowerCase()] : null;
    // INCLUSÃO DA VALIDAÇÃO DE ENTRADA DE BANDEIRAS
    const origem = req.query.origem ? req.query.origem.split(",") : null;
    const origemTuple = origem ? origem.map((valor) => valor.toUpperCase().replace(/-/g, ' ')) : null;

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
    // Validação dos parâmetros das origens
    if (origem !== null && origem.length === 1) query = query.replace('FROM RECEITA', `FROM ${origem}`);
    if (origemTuple) likeClause = origemTuple ? generateLikeClause('ORIGEM', origemTuple, conditions.join(' AND ')) : null;
    if (origem) query = query.replace(/--/g, ` WHERE ${likeClause}`);;
    if (conditions.length > 0 && origem === null) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);
    if (associados == 0) query = query.replace(`ASSOCIADO IN ("0")`, `ASSOCIADO = 0`);
    if (souabrasel == 0) query = query.replace(`SOU_ABRASEL IN ("0")`, `SOU_ABRASEL = 0`);
    query += `LIMIT ? OFFSET ?;`;

    db.all(query, [pageSize, offset], async (err, rows) => {
      if (err) {
        res.status(500).json({ error: err.message });
        return;
      }
      // Transforma a string em um array e remove os espaços em branco
      rows = rows.map(row => {
        row.ORIGEM = row.ORIGEM.split(',').map(element => element.trim());
        return row;
      });
      console.log(query)
      /*
      // Formatando data e hora para incluir no log
      const date = new Date()
      const formattedDate = `${date.getDate()}/${date.getMonth() + 1}/${date.getFullYear()}`; // formata a data como DD/MM/AAAA
      const formattedTime = `${date.getHours()}:${date.getMinutes()}:${date.getSeconds()}`; // formata a hora como HH:MM:SS
      dataChamada = `Data: ${formattedDate} - Hora: ${formattedTime}`
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${bandeira}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)
        */
      // Obter o formato desejado do parâmetro format
    const formato = req.query.formato ? req.query.formato.toLowerCase(): null;
    const cabecalho = ["CNPJ", "NOME_FANTASIA", "CIDADE", "BAIRRO", "ENDERECO", "TELEFONE", "EMAIL", "ASSOCIADO", "SOU_ABRASEL", "LINK_GOOGLE", "LINK_GOOGLE_MAPS"]
    if (formato === 'xlsx') {
      const xlsx = await jsonToXlsx(rows, cabecalho);
      res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
      res.status(200).send(
        xlsx
        );
    }
    // Se o formato for csv, converter os dados em csv e enviar na resposta
    else if (formato === 'csv') {
        const csv = jsonToCsv(rows, cabecalho);
        res.setHeader('Content-Type', 'text/csv');
        res.status(200).send(
          csv
          );
    }
    // Se o formato não for especificado ou for inválido, enviar os dados como json na resposta
    else {
        res.status(200).json(
          rows
          );
    }
    });
  })
// GetAll-contagem
router.route("/estabelecimentos/counts")
  .get(function(req, res, next) {
    const reqParams = JSON.stringify(req.query);
    lerCache(reqParams)
      .then(rows => {
        if (rows !== null) {
          // Se o cache existir e os parâmetros forem os mesmos, retorna os dados em cache
          return res.json(rows);
        } else {
          const clientIp = req.ip;
          const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
          //const bandeira = req.query.bandeira ? [req.query.bandeira.toLowerCase()] : null;
          // INCLUSÃO DA VALIDAÇÃO DE ENTRADA DE BANDEIRAS
          const origem = req.query.origem ? req.query.origem.split(",") : null;
          const origemTuple = origem ? origem.map((valor) => valor.toUpperCase().replace(/-/g, ' ')) : null;

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
          if (origem !== null && origem.length === 1) query = query.replace('FROM RECEITA', `FROM ${origem}`);
          if (origemTuple) likeClause = origemTuple ? generateLikeClause('ORIGEM', origemTuple, conditions.join(' AND ')) : null;
          if (origem) query = query.replace(/--/g, ` WHERE ${likeClause}`);;
          if (conditions.length > 0 && origem === null) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);

          if (groupby) query = query.replace(/\/\*\*\//g, ` GROUP BY ${groupby}`);
          if (groupby) query = query.replace(/COUNT\(\*\)/g, `${groupby}, COUNT(*)`);
          if (orderby) query = query.replace(";", `ORDER BY TOTAL ${orderby} ;`);
          if (associados == 0) query = query.replace(`ASSOCIADO IN ("0")`, `ASSOCIADO = 0`);
          if (souabrasel == 0) query = query.replace(`SOU_ABRASEL IN ("0")`, `SOU_ABRASEL = 0`);
          console.log(query)

          db.all(query, (err, rows) => {
            if (err) {
              res.status(500).json({ error: err.message });
              return;
            }
            // Testando a função com diferentes parâmetros
            if (groupby) rows = agrupar(rows, groupby);

            // Salve os dados no cache
            salvaCache(reqParams, rows);

            //console.log(rows)
            res.status(200).json(rows);
          });
        }
      }
    ).catch(error => {
        console.error('Erro ao ler cache:', error);
        res.status(500).json({ error: 'Erro ao ler cache.' });
  });
});
// GET BAIRROS 
router.route('/estabelecimentos/bairros') 
    .get(async function(req, res, next) { 
    const clientIp = req.ip; 
    const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
    // INCLUSÃO DA VALIDAÇÃO DE ENTRADA DE BANDEIRAS
    const origem = req.query.origem ? req.query.origem.split(",") : null;
    const origemTuple = origem ? origem.map((valor) => valor.toUpperCase().replace(/-/g, ' ')) : null;

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
    if (origem !== null && origem.length === 1) query = query.replace('FROM RECEITA', `FROM ${origem}`);
    if (origemTuple) likeClause = origemTuple ? generateLikeClause('ORIGEM', origemTuple, conditions.join(' AND ')) : null;
    if (origem) query = query.replace(/--/g, ` WHERE ${likeClause}`);
    if (conditions.length > 0 && origem === null) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);

    //console.log(query)
    db.all(query, async (err, rows) => {
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
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${origem}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)

    // Obter o formato desejado do parâmetro format
    const formato = req.query.formato ? req.query.formato.toLowerCase(): null;
    // Se o formato for xlsx, converter os dados em xlsx e enviar na resposta
    if (formato === 'xlsx') {
        const xlsx = await jsonToXlsx(rows);
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.status(200).send(xlsx);
    }
    // Se o formato for csv, converter os dados em csv e enviar na resposta
    else if (formato === 'csv') {
        const csv = jsonToCsv(rows);
        res.setHeader('Content-Type', 'text/csv');
        res.status(200).send(csv);
    }
    // Se o formato não for especificado ou for inválido, enviar os dados como json na resposta
    else {
        res.status(200).json(rows);
    }
    });
})

// get cidades
router.route('/estabelecimentos/cidades')
  .get(function(req, res, next) {
    const clientIp = req.ip;
    const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
    //const bandeira = req.query.bandeira ? [req.query.bandeira.toLowerCase()] : null;
    // INCLUSÃO DA VALIDAÇÃO DE ENTRADA DE BANDEIRAS
    const origem = req.query.origem ? req.query.origem.split(",") : null;
    const origemTuple = origem ? origem.map((valor) => valor.toUpperCase().replace(/-/g, ' ')) : null;

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
    if (origem !== null && origem.length === 1) query = query.replace('FROM RECEITA', `FROM ${origem}`);
    if (origemTuple) likeClause = origemTuple ? generateLikeClause('ORIGEM', origemTuple, conditions.join(' AND ')) : null;
    if (origem) query = query.replace(/--/g, ` WHERE ${likeClause}`);;
    if (conditions.length > 0 && origem === null) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);


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
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${origem}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)
      res.status(200).json(
        rows
      );
    });
  })

module.exports = router;
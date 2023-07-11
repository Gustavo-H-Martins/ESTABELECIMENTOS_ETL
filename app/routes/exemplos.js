const ExcelJS = require('exceljs');
const {Parser} = require('json2csv');
/**
 * Funções de apoio
 */

// INCLUSÃO DA FUNÇÃO PARA CONVERTER JSON EM CSV 
const jsonToCsv = (data) => { 
  /*
  // Definir as colunas do csv 
  const fields = ['BAIRRO']; 
  */
  // Obter as chaves do primeiro objeto como colunas do CSV
  const fields = Object.keys(data[0]);
  // Criar um parser com as opções desejadas 
  const parser = new Parser({ fields }); 
  
  // Converter os dados em csv 
  const csv = parser.parse(data); 
  
  // Retornar o csv como um buffer 
  return Buffer.from(csv); 
};

// INCLUSÃO DA FUNÇÃO PARA CONVERTER DADOS JSON EM XLSX
const jsonToXlsx = async (data) => {
  // Criar um novo workbook
  const workbook = new ExcelJS.Workbook();
  
  // Adicionar uma planilha ao workbook
  const worksheet = workbook.addWorksheet('Dados');
  
  // Converter os dados em um array de arrays
  const dataAsArray = data.map((obj) => Object.values(obj));
  
  // Adicionar os dados à planilha
  worksheet.addRows(dataAsArray);
  
  // Retornar o workbook como um stream
  return workbook.xlsx.writeBuffer();
};


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
 
// GET BAIRROS 
router.route('/estabelecimentos/bairros') 
    .get(async function(req, res, next) { 
    const clientIp = req.ip; 
    const cnpj = req.query.cnpj ? [req.query.cnpj.toUpperCase()] : null;
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
    if (bandeira !== null && bandeira.length === 1) query = query.replace('FROM RECEITA', `FROM ${bandeira}`);
    if (bandeiraTuple) likeClause = bandeiraTuple ? generateLikeClause('BANDEIRAS', bandeiraTuple, conditions.join(' AND ')) : null;
    if (bandeira) query = query.replace(/--/g, ` WHERE ${likeClause}`);
    if (conditions.length > 0 && bandeira === null) query = query.replace(/--/g, ` WHERE ${conditions.join(' AND ')}`);

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
      logToDatabase(clientIp, `Retornando ${rows.length} dados de "${bandeira}" no estado de "${uf}" cidade de ${cidade}`, 'INFO', dataChamada)

    // Obter o formato desejado do parâmetro format
    const format = req.query.format;
    // Se o formato for xlsx, converter os dados em xlsx e enviar na resposta
    if (format === 'xlsx') {
        const xlsx = await jsonToXlsx(rows);
        res.setHeader('Content-Type', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet');
        res.status(200).send(xlsx);
    }
    // Se o formato for csv, converter os dados em csv e enviar na resposta
    else if (format === 'csv') {
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

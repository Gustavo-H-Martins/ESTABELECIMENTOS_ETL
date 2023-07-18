// app/config/db.js (Camada Config)
/**
 * Libs
 */
const sqlite3 = require('sqlite3').verbose();
const path = require('path');

const dbFile = `${__dirname}`.replace('config', 'files/database.db')
// Configuração do servidor
// const dbFile = require('./variaveisAmbiente')
// Conectar ao banco de dados SQLite3
const db = new sqlite3.Database(dbFile, (err) => {
  if (err) {
    console.error(err.message);
  }
  console.log('conectado no banco de dados.');
  db.exec(`
    /* CRIANDO ÍNDICES PARA AS TABELAS QUE VÃO CRIAR AS VIEWS */
    CREATE INDEX IF NOT EXISTS idx_tb_rfb_local ON tb_rfb (RAZAO_SOCIAL, CIDADE, BAIRRO);
    CREATE INDEX IF NOT EXISTS idx_tb_rfb_cnpj ON tb_rfb (CNPJ);
    CREATE INDEX IF NOT EXISTS idx_tb_rfb_uf ON tb_rfb (UF);
    CREATE INDEX IF NOT EXISTS idx_tb_rfb_cidade ON tb_rfb (CIDADE);
    CREATE INDEX IF NOT EXISTS idx_tb_rfb_bairro ON tb_rfb (BAIRRO);
    
    CREATE INDEX IF NOT EXISTS idx_tb_cadastur_cnpj ON tb_cadastur (CNPJ);

    CREATE INDEX IF NOT EXISTS idx_tb_alelo ON tb_alelo (RAZAO_SOCIAL, CIDADE, BAIRRO);
    CREATE INDEX IF NOT EXISTS idx_tb_alelo_bandeira ON tb_alelo (BANDEIRA);
    CREATE INDEX IF NOT EXISTS idx_tb_benvisavale ON tb_benvisavale (RAZAO_SOCIAL, CIDADE, BAIRRO);
    CREATE INDEX IF NOT EXISTS idx_tb_benvisavale_bandeira ON tb_benvisavale (BANDEIRA);
    CREATE INDEX IF NOT EXISTS idx_tb_sodexo_bandeira ON tb_sodexo (BANDEIRA);
    CREATE INDEX IF NOT EXISTS idx_tb_sodexo ON tb_sodexo (RAZAO_SOCIAL, CIDADE, BAIRRO);
    CREATE INDEX IF NOT EXISTS idx_tb_ticket_bandeira ON tb_ticket (BANDEIRA);
    CREATE INDEX IF NOT EXISTS idx_tb_ticket ON tb_ticket (CNPJ);
    CREATE INDEX IF NOT EXISTS idx_tb_vr ON tb_vr (CNPJ);
    CREATE INDEX IF NOT EXISTS idx_tb_vr_bandeira ON tb_vr (BANDEIRA);
    
    CREATE VIEW IF NOT EXISTS  CADASTUR AS
        SELECT 
            C.CNPJ_FORMATADO AS CNPJ,
            C.RAZAO_SOCIAL AS RAZAO_SOCIAL,
            C.NOME_FANTASIA AS NOME_FANTASIA,
            C.CEP AS CEP,
            C.ENDERECO AS ENDERECO,
            C.BAIRRO AS BAIRRO,
            C.CIDADE AS CIDADE,
            C.UF AS UF,
            C.TELEFONE AS TELEFONE,
            RFB.TELEFONE AS TELEFONE_RFB,
            C.SITE AS SITE,
            C.INSTAGRAM AS INSTAGRAM,
            C.FACEBOOK AS FACEBOOK,
            C.URL_DETALHES_PRESTADOR AS URL_DETALHES_PRESTADOR,
            RFB.EMAIL,
            CASE
                WHEN C.CNPJ THEN "CADASTUR"
                ELSE False
            END AS ORIGEM,
            CASE 
                WHEN S.CNPJ IS NULL THEN False 
                ELSE True 
            END AS BASE_SIGA,
            CASE 
                WHEN S.ASSOCIADO IS NULL THEN False
                WHEN S.ASSOCIADO = 'ATIVO' THEN 'ATIVO'
                WHEN S.ASSOCIADO = 'INATIVO' THEN 'INATIVO'
            END AS ASSOCIADO,
            CASE
                WHEN S.SOU_ABRASEL IS NULL THEN False
                WHEN S.SOU_ABRASEL = 'ATIVO' THEN 'ATIVO'
                WHEN S.SOU_ABRASEL = 'INATIVO' THEN 'INATIVO'
                WHEN S.SOU_ABRASEL = 'CANCELADO' THEN 'CANCELADO'
            END AS SOU_ABRASEL
            M.LATITUDE AS LATITUDES,
            M.LONGITUDE AS LONGITUDES 
        FROM tb_cadastur C
        LEFT JOIN tb_rfb RFB ON C.CNPJ = RFB.CNPJ
        LEFT JOIN tb_siga S ON S.CNPJ = C.CNPJ;
        LEFT JOIN tb_municipios M ON M.CIDADE = C.CIDADE AND M.UF = C.UF AND M.BAIRRO = C.BAIRRO;
    CREATE VIEW IF NOT EXISTS  TICKET AS
        SELECT 
            RFB.CNPJ AS CNPJ,
            RFB.RAZAO_SOCIAL AS RAZAO_SOCIAL_RFB,
            T.ESTABELECIMENTOS AS NOME_FANTASIA,
            T.CEP AS CEP,
            T.ENDERECO AS ENDERECO,
            T.BAIRRO AS BAIRRO,
            T.CIDADE AS CIDADE,
            T.UF AS UF,
            T.TELEFONE AS TELEFONE,
            RFB.TELEFONE AS TELEFONE_RFB,
            RFB.EMAIL,
            T.BANDEIRA || COALESCE(", " || V.BANDEIRA, ' ') 
            || COALESCE(", " || A.BANDEIRA, ' ') || COALESCE(", " 
            || SO.BANDEIRA, ' ') ||COALESCE(", " || B.BANDEIRA, ' ') AS ORIGEM,
            
            /*
            CASE 
                WHEN T.CNPJ IS NULL THEN False 
                ELSE True
            END AS TEM_TICKET,
            CASE 
                WHEN V.CNPJ IS NULL THEN False 
                ELSE True
            END AS TEM_VR,
            CASE 
                WHEN SO.RAZAO_SOCIAL IS NULL THEN False 
                ELSE True
            END AS TEM_SODEXO,
            CASE 
                WHEN A.RAZAO_SOCIAL IS NULL THEN False
                ELSE True
            END AS TEM_ALELO,
            CASE 
                WHEN B.RAZAO_SOCIAL IS NULL THEN False
                ELSE True
            END AS TEM_BENVISAVALE,
            */
            CASE 
                WHEN S.CNPJ IS NULL THEN False 
                ELSE True 
            END AS BASE_SIGA,
            CASE 
                WHEN S.ASSOCIADO IS NULL THEN False
                WHEN S.ASSOCIADO = 'ATIVO' THEN 'ATIVO'
                WHEN S.ASSOCIADO = 'INATIVO' THEN 'INATIVO'
            END AS ASSOCIADO,
            CASE
                WHEN S.SOU_ABRASEL IS NULL THEN False
                WHEN S.SOU_ABRASEL = 'ATIVO' THEN 'ATIVO'
                WHEN S.SOU_ABRASEL = 'INATIVO' THEN 'INATIVO'
                WHEN S.SOU_ABRASEL = 'CANCELADO' THEN 'CANCELADO'
            END AS SOU_ABRASEL,
            T.LATITUDE AS LATITUDES,
            T.LONGITUDE AS LONGITUDES
        FROM tb_ticket T
        LEFT JOIN tb_rfb RFB ON T.CNPJ = RFB.CNPJ
        LEFT JOIN tb_alelo A ON A.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL AND A.BAIRRO = RFB.BAIRRO AND A.CIDADE = RFB.CIDADE
        LEFT JOIN tb_vr V ON V.CNPJ = RFB.CNPJ
        LEFT JOIN tb_sodexo SO ON SO.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL AND SO.BAIRRO = RFB.BAIRRO AND SO.CIDADE = RFB.CIDADE
        LEFT JOIN tb_benvisavale B ON B.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL  AND B.BAIRRO = RFB.BAIRRO AND B.CIDADE = RFB.CIDADE
        LEFT JOIN tb_siga S ON S.CNPJ = RFB.CNPJ;
        
    CREATE VIEW IF NOT EXISTS  ALELO AS
        SELECT 
            RFB.CNPJ AS CNPJ,
            RFB.RAZAO_SOCIAL AS RAZAO_SOCIAL_RFB,
            A.ESTABELECIMENTOS AS NOME_FANTASIA,
            A.CEP AS CEP,
            A.ENDERECO AS ENDERECO,
            A.BAIRRO AS BAIRRO,
            A.CIDADE AS CIDADE,
            A.UF AS UF,
            A.TELEFONE AS TELEFONE,
            RFB.TELEFONE AS TELEFONE_RFB,
            RFB.EMAIL,
            A.BANDEIRA || COALESCE(", " || V.BANDEIRA, ' ') 
            || COALESCE(", " || T.BANDEIRA, ' ') || COALESCE(", " 
            || SO.BANDEIRA, ' ') ||COALESCE(", " || B.BANDEIRA, ' ') AS ORIGEM,
            
            /*
            CASE 
                WHEN T.CNPJ IS NULL THEN False 
                ELSE True
            END AS TEM_TICKET,
            CASE 
                WHEN V.CNPJ IS NULL THEN False 
                ELSE True
            END AS TEM_VR,
            CASE 
                WHEN SO.RAZAO_SOCIAL IS NULL THEN False 
                ELSE True
            END AS TEM_SODEXO,
            CASE 
                WHEN A.RAZAO_SOCIAL IS NULL THEN False
                ELSE True
            END AS TEM_ALELO,
            CASE 
                WHEN B.RAZAO_SOCIAL IS NULL THEN False
                ELSE True
            END AS TEM_BENVISAVALE,
            */
            CASE 
                WHEN S.CNPJ IS NULL THEN False 
                ELSE True 
            END AS BASE_SIGA,
            CASE 
                WHEN S.ASSOCIADO IS NULL THEN False
                WHEN S.ASSOCIADO = 'ATIVO' THEN 'ATIVO'
                WHEN S.ASSOCIADO = 'INATIVO' THEN 'INATIVO'
            END AS ASSOCIADO,
            CASE
                WHEN S.SOU_ABRASEL IS NULL THEN False
                WHEN S.SOU_ABRASEL = 'ATIVO' THEN 'ATIVO'
                WHEN S.SOU_ABRASEL = 'INATIVO' THEN 'INATIVO'
                WHEN S.SOU_ABRASEL = 'CANCELADO' THEN 'CANCELADO'
            END AS SOU_ABRASEL,
            A.LATITUDE AS LATITUDES,
            A.LONGITUDE AS LONGITUDES
        FROM tb_alelo A 
        LEFT JOIN tb_rfb RFB ON RFB.RAZAO_SOCIAL = A.RAZAO_SOCIAL AND RFB.BAIRRO = A.BAIRRO AND RFB.CIDADE = A.CIDADE
        LEFT JOIN tb_ticket T ON T.CNPJ = RFB.CNPJ 
        LEFT JOIN tb_vr V ON V.CNPJ = RFB.CNPJ
        LEFT JOIN tb_sodexo SO ON SO.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL AND SO.BAIRRO = RFB.BAIRRO AND SO.CIDADE = RFB.CIDADE
        LEFT JOIN tb_benvisavale B ON B.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL  AND B.BAIRRO = RFB.BAIRRO AND B.CIDADE = RFB.CIDADE
        LEFT JOIN tb_siga S ON S.CNPJ = RFB.CNPJ;
        
    CREATE VIEW IF NOT EXISTS  VR AS
        SELECT 
            RFB.CNPJ AS CNPJ,
            RFB.RAZAO_SOCIAL AS RAZAO_SOCIAL_RFB,
            V.ESTABELECIMENTOS AS NOME_FANTASIA,
            V.CEP AS CEP,
            V.ENDERECO AS ENDERECO,
            V.BAIRRO AS BAIRRO,
            V.CIDADE AS CIDADE,
            V.UF AS UF,
            V.TELEFONE AS TELEFONE,
            RFB.TELEFONE AS TELEFONE_RFB,
            RFB.EMAIL,
            V.BANDEIRA || COALESCE(", " || T.BANDEIRA, ' ') 
            || COALESCE(", " || A.BANDEIRA, ' ') || COALESCE(", " 
            || SO.BANDEIRA, ' ') ||COALESCE(", " || B.BANDEIRA, ' ') AS ORIGEM,
            
            /*
            CASE 
                WHEN T.CNPJ IS NULL THEN False 
                ELSE True
            END AS TEM_TICKET,
            CASE 
                WHEN V.CNPJ IS NULL THEN False 
                ELSE True
            END AS TEM_VR,
            CASE 
                WHEN SO.RAZAO_SOCIAL IS NULL THEN False 
                ELSE True
            END AS TEM_SODEXO,
            CASE 
                WHEN A.RAZAO_SOCIAL IS NULL THEN False
                ELSE True
            END AS TEM_ALELO,
            CASE 
                WHEN B.RAZAO_SOCIAL IS NULL THEN False
                ELSE True
            END AS TEM_BENVISAVALE,
            */
            CASE 
                WHEN S.CNPJ IS NULL THEN False 
                ELSE True 
            END AS BASE_SIGA,
            CASE 
                WHEN S.ASSOCIADO IS NULL THEN False
                WHEN S.ASSOCIADO = 'ATIVO' THEN 'ATIVO'
                WHEN S.ASSOCIADO = 'INATIVO' THEN 'INATIVO'
            END AS ASSOCIADO,
            CASE
                WHEN S.SOU_ABRASEL IS NULL THEN False
                WHEN S.SOU_ABRASEL = 'ATIVO' THEN 'ATIVO'
                WHEN S.SOU_ABRASEL = 'INATIVO' THEN 'INATIVO'
                WHEN S.SOU_ABRASEL = 'CANCELADO' THEN 'CANCELADO'
            END AS SOU_ABRASEL,
            V.LATITUDE AS LATITUDES,
            V.LONGITUDE AS LONGITUDES
        FROM tb_vr V
        LEFT JOIN tb_rfb RFB ON V.CNPJ = RFB.CNPJ
        LEFT JOIN tb_alelo A ON A.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL AND A.BAIRRO = RFB.BAIRRO AND A.CIDADE = RFB.CIDADE
        LEFT JOIN tb_ticket T  ON T.CNPJ = RFB.CNPJ
        LEFT JOIN tb_sodexo SO ON SO.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL AND SO.BAIRRO = RFB.BAIRRO AND SO.CIDADE = RFB.CIDADE
        LEFT JOIN tb_benvisavale B ON B.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL  AND B.BAIRRO = RFB.BAIRRO AND B.CIDADE = RFB.CIDADE
        LEFT JOIN tb_siga S ON S.CNPJ = RFB.CNPJ;
        
    CREATE VIEW IF NOT EXISTS  SODEXO AS
        SELECT 
            RFB.CNPJ AS CNPJ,
            RFB.RAZAO_SOCIAL AS RAZAO_SOCIAL_RFB,
            SO.ESTABELECIMENTOS AS NOME_FANTASIA,
            SO.CEP AS CEP,
            SO.ENDERECO AS ENDERECO,
            SO.BAIRRO AS BAIRRO,
            SO.CIDADE AS CIDADE,
            SO.UF AS UF,
            SO.TELEFONE AS TELEFONE,
            RFB.TELEFONE AS TELEFONE_RFB,
            RFB.EMAIL,
            SO.BANDEIRA || COALESCE(", " || T.BANDEIRA, ' ') 
            || COALESCE(", " || A.BANDEIRA, ' ') || COALESCE(", " 
            || V.BANDEIRA, ' ') ||COALESCE(", " || B.BANDEIRA, ' ') AS ORIGEM,
            
            /*
            CASE 
                WHEN T.CNPJ IS NULL THEN False 
                ELSE True
            END AS TEM_TICKET,
            CASE 
                WHEN V.CNPJ IS NULL THEN False 
                ELSE True
            END AS TEM_VR,
            CASE 
                WHEN SO.RAZAO_SOCIAL IS NULL THEN False 
                ELSE True
            END AS TEM_SODEXO,
            CASE 
                WHEN A.RAZAO_SOCIAL IS NULL THEN False
                ELSE True
            END AS TEM_ALELO,
            CASE 
                WHEN B.RAZAO_SOCIAL IS NULL THEN False
                ELSE True
            END AS TEM_BENVISAVALE,
            */
            CASE 
                WHEN S.CNPJ IS NULL THEN False 
                ELSE True 
            END AS BASE_SIGA,
            CASE 
                WHEN S.ASSOCIADO IS NULL THEN False
                WHEN S.ASSOCIADO = 'ATIVO' THEN 'ATIVO'
                WHEN S.ASSOCIADO = 'INATIVO' THEN 'INATIVO'
            END AS ASSOCIADO,
            CASE
                WHEN S.SOU_ABRASEL IS NULL THEN False
                WHEN S.SOU_ABRASEL = 'ATIVO' THEN 'ATIVO'
                WHEN S.SOU_ABRASEL = 'INATIVO' THEN 'INATIVO'
                WHEN S.SOU_ABRASEL = 'CANCELADO' THEN 'CANCELADO'
            END AS SOU_ABRASEL,
            SO.LATITUDE AS LATITUDES,
            SO.LONGITUDE AS LONGITUDES
        FROM  tb_sodexo SO
        LEFT JOIN tb_rfb RFB ON SO.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL AND SO.BAIRRO = RFB.BAIRRO AND SO.CIDADE = RFB.CIDADE 
        LEFT JOIN tb_alelo A ON A.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL AND A.BAIRRO = RFB.BAIRRO AND A.CIDADE = RFB.CIDADE
        LEFT JOIN tb_ticket T  ON T.CNPJ = RFB.CNPJ
        LEFT JOIN tb_vr V ON V.CNPJ = RFB.CNPJ
        LEFT JOIN tb_benvisavale B ON B.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL  AND B.BAIRRO = RFB.BAIRRO AND B.CIDADE = RFB.CIDADE
        LEFT JOIN tb_siga S ON S.CNPJ = RFB.CNPJ;
        
    CREATE VIEW IF NOT EXISTS  BENVISAVALE AS
        SELECT 
            RFB.CNPJ AS CNPJ,
            RFB.RAZAO_SOCIAL AS RAZAO_SOCIAL_RFB,
            B.ESTABELECIMENTOS AS NOME_FANTASIA,
            B.CEP AS CEP,
            B.ENDERECO AS ENDERECO,
            B.BAIRRO AS BAIRRO,
            B.CIDADE AS CIDADE,
            B.UF AS UF,
            B.TELEFONE AS TELEFONE,
            RFB.TELEFONE AS TELEFONE_RFB,
            RFB.EMAIL,
            B.BANDEIRA || COALESCE(", " || T.BANDEIRA, ' ') 
            || COALESCE(", " || A.BANDEIRA, ' ') || COALESCE(", " 
            || V.BANDEIRA, ' ') ||COALESCE(", " || SO.BANDEIRA, ' ') AS ORIGEM,
            
            /*
            CASE 
                WHEN T.CNPJ IS NULL THEN False 
                ELSE True
            END AS TEM_TICKET,
            CASE 
                WHEN V.CNPJ IS NULL THEN False 
                ELSE True
            END AS TEM_VR,
            CASE 
                WHEN SO.RAZAO_SOCIAL IS NULL THEN False 
                ELSE True
            END AS TEM_SODEXO,
            CASE 
                WHEN A.RAZAO_SOCIAL IS NULL THEN False
                ELSE True
            END AS TEM_ALELO,
            CASE 
                WHEN B.RAZAO_SOCIAL IS NULL THEN False
                ELSE True
            END AS TEM_BENVISAVALE,
            */
            CASE 
                WHEN S.CNPJ IS NULL THEN False 
                ELSE True 
            END AS BASE_SIGA,
            CASE 
                WHEN S.ASSOCIADO IS NULL THEN False
                WHEN S.ASSOCIADO = 'ATIVO' THEN 'ATIVO'
                WHEN S.ASSOCIADO = 'INATIVO' THEN 'INATIVO'
            END AS ASSOCIADO,
            CASE
                WHEN S.SOU_ABRASEL IS NULL THEN False
                WHEN S.SOU_ABRASEL = 'ATIVO' THEN 'ATIVO'
                WHEN S.SOU_ABRASEL = 'INATIVO' THEN 'INATIVO'
                WHEN S.SOU_ABRASEL = 'CANCELADO' THEN 'CANCELADO'
            END AS SOU_ABRASEL,
            B.LATITUDE AS LATITUDES,
            B.LONGITUDE AS LONGITUDES
        FROM  tb_benvisavale B
        LEFT JOIN tb_rfb RFB ON B.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL  AND B.BAIRRO = RFB.BAIRRO AND B.CIDADE = RFB.CIDADE
        LEFT JOIN tb_alelo A ON A.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL AND A.BAIRRO = RFB.BAIRRO AND A.CIDADE = RFB.CIDADE
        LEFT JOIN tb_ticket T  ON T.CNPJ = RFB.CNPJ
        LEFT JOIN tb_vr V ON V.CNPJ = RFB.CNPJ
        LEFT JOIN  tb_sodexo SO ON SO.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL AND SO.BAIRRO = RFB.BAIRRO AND SO.CIDADE = RFB.CIDADE  
        LEFT JOIN tb_siga S ON S.CNPJ = RFB.CNPJ;
        
    CREATE VIEW IF NOT EXISTS  RECEITA AS
        SELECT 
            RFB.CNPJ AS CNPJ,
            RFB.RAZAO_SOCIAL AS RAZAO_SOCIAL_RFB,
            RFB.NOME_FANTASIA AS NOME_FANTASIA,
            RFB.CEP AS CEP,
            RFB.ENDERECO AS ENDERECO,
            RFB.BAIRRO AS BAIRRO,
            RFB.CIDADE AS CIDADE,
            RFB.UF AS UF,
            B.TELEFONE AS TELEFONE,
            RFB.TELEFONE AS TELEFONE_RFB,
            RFB.EMAIL,
            COALESCE(""|| COALESCE("" || B.BANDEIRA, ' ') || COALESCE(", " || T.BANDEIRA, ' ') 
            || COALESCE(", " || A.BANDEIRA, ' ') || COALESCE(", " 
            || V.BANDEIRA, ' ') ||COALESCE(", " || SO.BANDEIRA, ' '), NULL) AS ORIGEM,
            
            /*
            CASE 
                WHEN T.CNPJ IS NULL THEN False 
                ELSE True
            END AS TEM_TICKET,
            CASE 
                WHEN V.CNPJ IS NULL THEN False 
                ELSE True
            END AS TEM_VR,
            CASE 
                WHEN SO.RAZAO_SOCIAL IS NULL THEN False 
                ELSE True
            END AS TEM_SODEXO,
            CASE 
                WHEN A.RAZAO_SOCIAL IS NULL THEN False
                ELSE True
            END AS TEM_ALELO,
            CASE 
                WHEN B.RAZAO_SOCIAL IS NULL THEN False
                ELSE True
            END AS TEM_BENVISAVALE,
            */
            CASE 
                WHEN S.CNPJ IS NULL THEN False 
                ELSE True 
            END AS BASE_SIGA,
            CASE 
                WHEN S.ASSOCIADO IS NULL THEN False
                WHEN S.ASSOCIADO = 'ATIVO' THEN 'ATIVO'
                WHEN S.ASSOCIADO = 'INATIVO' THEN 'INATIVO'
            END AS ASSOCIADO,
            CASE
                WHEN S.SOU_ABRASEL IS NULL THEN False
                WHEN S.SOU_ABRASEL = 'ATIVO' THEN 'ATIVO'
                WHEN S.SOU_ABRASEL = 'INATIVO' THEN 'INATIVO'
                WHEN S.SOU_ABRASEL = 'CANCELADO' THEN 'CANCELADO'
            END AS SOU_ABRASEL,
            M.LATITUDE AS LATITUDES,
            M.LONGITUDE AS LONGITUDES
        FROM tb_rfb RFB
        LEFT JOIN tb_benvisavale B ON B.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL  AND B.BAIRRO = RFB.BAIRRO AND B.CIDADE = RFB.CIDADE
        LEFT JOIN tb_alelo A ON A.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL AND A.BAIRRO = RFB.BAIRRO AND A.CIDADE = RFB.CIDADE
        LEFT JOIN tb_ticket T  ON T.CNPJ = RFB.CNPJ
        LEFT JOIN tb_vr V ON V.CNPJ = RFB.CNPJ
        LEFT JOIN  tb_sodexo SO ON SO.RAZAO_SOCIAL = RFB.RAZAO_SOCIAL AND SO.BAIRRO = RFB.BAIRRO AND SO.CIDADE = RFB.CIDADE  
        LEFT JOIN tb_siga S ON S.CNPJ = RFB.CNPJ
        LEFT JOIN tb_municipios M ON M.CIDADE = RFB.CIDADE AND M.UF = RFB.UF AND M.BAIRRO = RFB.BAIRRO;
    `, function(err) {
    if (err) {
      console.error(err.message)
    } else {
      console.log("Views criadas com sucesso!")
    }
  });
});

module.exports = db;
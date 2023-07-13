# Api de acesso aos dados de cartões benefícios
        Este repositório serve de modelo de aplicação da api de leads da Abrasel com base nos cartões benefícios parceiros.
![Static Badge](https://img.shields.io/badge/Desenvolvimento_-0.1.2-red)  ![Static Badge](https://img.shields.io/badge/Producao_-0.1.0-green) 
## Pré-requisitos

Antes de começar, você vai precisar ter instalado em sua máquina as seguintes ferramentas:

* ### [Git](https://git-scm.com/)
* ### [Node.js](https://nodejs.org/en/)
* ### [SQLite](https://www.npmjs.com/package/sqlite3)

Além disso, é recomendável ter um editor de código, como o [Visual Studio Code](https://code.visualstudio.com/).

## Como instalar
```bash
# Clone este repositório
$ git clone https://github.com/Gustavo-H-Martins/coletor_leads_vouchers/tree/master/app

# Acesse a pasta do projeto no terminal/cmd
$ cd api

# Instale as dependências
$ npm install
```

## Como executar

```bash
# Execute a aplicação em modo de desenvolvimento
$ npm start

# O servidor iniciará na porta definida no arquivo app.js. Ex: http://localhost:3000
```

## Rotas da API
A API possui as seguintes [rotas](./routes/leads.js):

* #### GET `/api/v2/leads/estabelecimentos/` 
Este endpoint retorna uma lista de estabelecimentos. Você pode filtrar os resultados usando os parâmetros de consulta: 
- `bandeira` 
- `uf`
- `cidade` 
- `bairros` 
- `associados`
- `souabrasel`
- `lat`
- `lon`
- `raio`


Esses parâmetros são opcionais e podem ser usados em qualquer combinação. Por exemplo, você pode fornecer apenas o parâmetro `bandeira` para retornar estabelecimentos de uma determinada bandeira em todos os estados e cidades.

Você também pode usar os parâmetros de consulta `page` e `pageSize` para implementar a paginação dos resultados. O parâmetro `page` especifica o número da página a ser retornada, enquanto o parâmetro `pageSize` especifica o número de resultados por página. Se esses parâmetros não forem fornecidos, serão usados valores padrão de `1` e `100`, respectivamente.

O endpoint aceita um cabeçalho opcional `x-forwarded-for`, que pode ser usado para especificar o endereço IP do cliente.

O endpoint retorna um código de status HTTP 200 em caso de sucesso, juntamente com um array JSON contendo os estabelecimentos correspondentes aos critérios de pesquisa, passando o parâmetro `formato` =XLSX ou =CSV retorna um arquivo nos formatos solicitados para download. Em caso de erro, o endpoint retorna um código de status HTTP 500 com uma mensagem de erro.

exemplos de uso

        /estabelecimentos?bandeira=sodexo&uf=SP&cidade=Sao-Paulo&page=2&pageSize=20
        /estabelecimentos?bandeira=sodexo&uf=SP&page=3
        /estabelecimentos?uf=RJ
        /estabelecimentos
* #### GET `/api/v2/leads/estabelecimentos/counts` 
Esse endpoint possui os mesmos filtros do endpoint supracitado, com a diferença que os dados aqui retornados são contagens de ocorrencias pelos filtros passados.

Há também os parâmetros adicionais:
- `groupby`
- `orderby`
Usados para ordenação dos dados retornados pela api, por padrão o order by é desc pelo campo `TOTAL`

* #### GET `/api/v2/leads/estabelecimentos/bairros`
Retorna uma lista de bairros onde há pelo menos um dos leads da base.
* #### GET `/api/v2/leads//estabelecimentos/cidades`
Retorna uma lista de das cidades onde há pelo menos um dos leads da base


## Documentação da API
A documentação da API pode ser encontrada na rota `/api/v2/leads/docs`, onde é possível visualizar a lista de rotas, parâmetros e exemplos de requisições e respostas.

Contribuição
Faça um fork do projeto.
Crie uma nova branch com suas alterações: `git checkout -b minha-feature`
Salve suas alterações e faça um commit: `git commit -m "Minha feature"`
Envie suas alterações para o seu fork: `git push origin minha-feature`
Crie um pull request para o repositório original.

[![Gustavo-H-Martins](https://github-readme-stats.vercel.app/api?username=Gustavo-H-Martins&show_icons=true&theme=radical)](https://github.com/Gustavo-H-Martins)
## Licença
Este projeto não tem licença de uso mais. Consulte o arquivo [LICENSE](./licence) para mais detalhes.

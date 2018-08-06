import conexaoTwitter
from twitter import error
import time
from datetime import datetime, timedelta
from time import strptime
import os
from pathlib import Path


class Coleta(object):
    def __init__(self):
        # ABERTURA DE CONEXÃO COM O TWITTER
        self.api = conexaoTwitter.Open()

        # CRIAÇÃO DE DICIONÁRIOS
        self.dicRespostas = {}
        self.dicMencoes = {}
        self.dicSeguidores = {}
        self.dicLimites = {}
        self.dicSeguidoresVinculados = {}
        self.dicSeguidoresDesvinculados = {}
        self.dicPosts = {}

    def teste(self):
        pass

    def realizar_coleta(self):
        if self.api is not None:

            lista_de_bots = self.obter_bots()
            self.primeira_execucao(lista_de_bots)

            while True:
                # HORÁRIO DE INÍCIO DA COLETA
                inicio_coleta = datetime.now()

                lista_de_bots = self.obter_bots()

                for bot in lista_de_bots:
                    # DADOS DO BOT
                    dados_bot = self.obter_dados_bot(bot)
                    
                    if dados_bot is None:
                        continue

                    nome_bot = dados_bot[0]

                    print("Coleta iniciada para o bot " + str(nome_bot))

                    # COLETA DE SEGUIDORES
                    seguidores = self.api.GetFollowerIDs(user_id=bot)
                    coleta_seguidores = datetime.now()
                    print(str(len(seguidores)) + " seguidores coletados")

                    # ESTRUTURAÇÃO DOS DICIONÁRIOS
                    for seguidor in seguidores:
                        self.dicSeguidores[seguidor] = {'coletado_em': str(coleta_seguidores)}
                        self.dicRespostas[seguidor] = []
                        self.dicMencoes[seguidor] = []

                    self.valida_vinculacao_desvinculacao(bot, seguidores)

                    # DEFINE LIMITES DE CONSULTAS À API
                    limites = self.obter_limites(bot)
                    for limite in limites:
                        linha = limite.split(",")
                        if linha[0] == "interacoes":
                            self.dicLimites['interacoes'] = linha[1]
                        elif linha[0] == "posts":
                            self.dicLimites['posts'] = linha[1]
                    print("Limites de consulta definidos")

                    # COLETA DE POSTS
                    posts = self.obter_posts_bot(bot, int(self.dicLimites['posts']))
                    print(str(len(posts)) + " novos posts coletados")

                    posts_antigos = self.obter_posts_antigos(bot)
                    posts_antigos = list(map(int, posts_antigos))  # CONVERTE ITENS DA LISTA PARA INTEIROS

                    print(str(len(posts_antigos) + len(posts)) + " posts para analisar")

                    id_posts = []
                    for post in posts:
                        id_posts.append(post.id)

                    id_posts = id_posts + posts_antigos

                    # COLETA DE MENCOES E RESPOSTAS
                    self.obter_mencoes_respostas_bot(nome_bot, id_posts, int(self.dicLimites['interacoes']))
                    print("Menções e respostas coletadas")

                    self.salvar_dicionario(bot, "seguidores")
                    self.salvar_dicionario(bot, "respostas")
                    self.salvar_dicionario(bot, "mencoes")
                    self.salvar_dicionario(bot, "limites")
                    self.salvar_dicionario(bot, "seguidoresVinculacao")
                    self.salvar_dicionario(bot, "seguidoresDesvinculacao")
                    self.salvar_dicionario(bot, "posts")
                    print("Dados salvos")

                    self.bot_coletado(bot)
                    self.limpar_dicionarios()
                    print("Coleta finalizada para o bot " + str(bot))

                self.limpar_coletados()
                # ESPERA 1 DIA A PARTIR DO INÍCIO DA COLETA PARA EXECUTAR NOVAMENTE
                fim_coleta = datetime.now()
                tempo_processamento_coleta = (fim_coleta - inicio_coleta).total_seconds()
                if tempo_processamento_coleta < 86400:
                    print("Coleta concluída em " + str(datetime.now()))
                    print("Próxima coleta será executada em " +
                          str(datetime.now() + timedelta(0, 86400 - tempo_processamento_coleta)))
                    time.sleep(86400 - tempo_processamento_coleta)
        else:
            print("Erro ao acessar a API.")

    # PRIMEIRA EXECUÇÃO DO SCRIPT (SALVA DADOS QUE NÃO VARIAM)
    def primeira_execucao(self, lista_de_bots):
        if self.primeira_execucao_feita() is False:
            for bot in lista_de_bots:
                # DADOS DO BOT
                dados_bot = self.obter_dados_bot(bot)

                if dados_bot is None:
                    continue

                criacao_bot = dados_bot[1]

                # CRIAÇÃO DA PERSISTÊNCIA DO ROBÔ
                self.criar_pasta_bot(bot)
                self.criar_arquivos_bot(bot, criacao_bot)
            self.salva_primeira_execucao()

    # SALVA DICIONÁRIO COM OS DADOS COLETADOS
    def salvar_dicionario(self, bot, tipo):

        if tipo == "limites" or tipo == "seguidores":
            arquivo = open("ArquivosSaida/" + str(bot) + "/" + tipo + ".txt", "w")
        else:
            arquivo = open("ArquivosSaida/" + str(bot) + "/" + tipo + ".txt", "a")

        if tipo == "respostas":
            for resposta in self.dicRespostas:
                for i in range(0, len(self.dicRespostas[resposta])):
                    arquivo.write(str(resposta) + "," + str(self.dicRespostas[resposta][i]['id']) +
                                  "," + str(self.dicRespostas[resposta][i]['data']) + "\n")
        elif tipo == "seguidores":
            arquivo.write("id,screenName,dataCriacao,numeroLikes\n")
            for seguidor in self.dicSeguidores:
                arquivo.write(str(seguidor) + "\n")
        elif tipo == "seguidoresVinculacao":
            for seguidor in self.dicSeguidoresVinculados:
                arquivo.write(str(seguidor) + "," +
                              str(self.dicSeguidoresVinculados[seguidor]['vinculacao']) + "\n")
        elif tipo == "seguidoresDesvinculacao":
            for seguidor in self.dicSeguidoresDesvinculados:
                arquivo.write(str(seguidor) + "," +
                              str(self.dicSeguidoresDesvinculados[seguidor]['desvinculacao']) + "\n")
        elif tipo == "mencoes":
            for mencao in self.dicMencoes:
                for i in range(0, len(self.dicMencoes[mencao])):
                    arquivo.write(str(mencao) + "," + str(self.dicMencoes[mencao][i]['id']) +
                                  "," + str(self.dicMencoes[mencao][i]['data']) + "\n")
        elif tipo == "limites":
            arquivo.write("tipoLimite,valor\n")
            for limite in self.dicLimites:
                arquivo.write(str(limite) + "," + str(self.dicLimites[limite]) + "\n")
        elif tipo == "posts":
            for post in self.dicPosts:
                arquivo.write(str(post) + "," + str(self.dicPosts[post]) + "\n")
        else:
            print("Tipo inesperado")

        arquivo.close()

    def limpar_dicionarios(self):
        self.dicRespostas.clear()
        self.dicMencoes.clear()
        self.dicSeguidores.clear()
        self.dicLimites.clear()
        self.dicSeguidoresVinculados.clear()
        self.dicSeguidoresDesvinculados.clear()
        self.dicPosts.clear()

    @staticmethod
    def salvar_posts(bot, posts):
        arquivo = open("ArquivosSaida/" + str(bot) + "/posts.txt", "a")
        for post in posts:
            arquivo.write(str(post) + "\n")
        arquivo.close()

    def obter_mencoes_respostas_bot(self, nome_bot, id_posts, min_id):
        max_id = 9000000000000000000
        horario_coleta = None

        limite_interacao = 0
        while True:
            try:
                horario_coleta = datetime.now()
                # Pega os posts do bot e armazena em lista
                mencoes_bot = self.api.GetRepliesToUser(screen_name_bot=nome_bot, max_id=max_id, since_id=min_id)

                if len(mencoes_bot) > 0:
                    if mencoes_bot[0].id > limite_interacao:
                        limite_interacao = mencoes_bot[0].id

                    for m in mencoes_bot:
                        if m.user.id in self.dicSeguidores.keys():
                            if m.in_reply_to_status_id in id_posts:
                                self.dicRespostas[m.user.id].append({'id': m.id,
                                                                     'data': self.converter_formato_data(m.created_at)})
                            else:
                                self.dicMencoes[m.user.id].append({'id': m.id,
                                                                   'data': self.converter_formato_data(m.created_at)})

            except error.TwitterError as e:
                print("Erro durante coleta de posts: " + str(e.message))

            # Termina a coleta se não houverem mais posts
            if len(mencoes_bot) == 0:
                break
            else:
                # Define um novo limite para nova coleta
                if len(mencoes_bot) > 0:
                    max_id = mencoes_bot[len(mencoes_bot) - 1].id - 1

            # Aguarda tempo para não exceder limite da API
            horario_corrente = datetime.now()
            tempo_processamento = (horario_corrente - horario_coleta).total_seconds()
            {} if tempo_processamento > 5 else time.sleep(5 - tempo_processamento)

        if limite_interacao > 0:
            self.dicLimites['interacoes'] = limite_interacao

    def obter_dados_bot(self, bot):
        try:
            objeto_bot = self.api.GetUser(user_id=bot)
            return [objeto_bot.screen_name, objeto_bot.created_at]
        except error.TwitterError as e:
            print("Erro durante coleta de dados do usuário: " + str(e.message))

    @staticmethod
    def obter_limites(bot):
        try:
            file = open("ArquivosSaida/" + str(bot) + "/limites.txt", 'r')
            lines = file.readlines()
            file.close()

            return lines
        except IOError as e:
            print("Erro ao ler os limites: " + str(e))

    @staticmethod
    def obter_seguidor_vinculacao(bot):
        try:
            file = open("ArquivosSaida/" + str(bot) + "/seguidoresVinculacao.txt", 'r')
            lines = file.readlines()
            file.close()

            lines.pop(0)
            retorno = []
            for line in lines:
                dados = line.split(",")
                retorno.append(int(dados[0]))
            return retorno
        except IOError as e:
            print("Erro ao ler os limites: " + str(e))

    @staticmethod
    def obter_seguidor_desvinculacao(bot):
        try:
            file = open("ArquivosSaida/" + str(bot) + "/seguidoresDesvinculacao.txt", 'r')
            lines = file.readlines()
            file.close()

            lines.pop(0)
            retorno = []
            for line in lines:
                dados = line.split(",")
                retorno.append(int(dados[0]))
            return retorno
        except IOError as e:
            print("Erro ao ler os limites: " + str(e))

    @staticmethod
    def obter_posts_antigos(bot):
        try:
            file = open("ArquivosSaida/" + str(bot) + "/posts.txt", 'r')
            lines = file.readlines()
            file.close()

            id_posts = []
            for line in lines:
                line_splitted = line.split(",")
                id_posts.append(line_splitted[0])

            return id_posts
        except IOError as e:
            print("Erro ao ler os posts antigos: " + str(e))

    def obter_retweets(self, posts):
        try:
            for post in posts:
                horario_coleta = datetime.now()
                retweets_post = self.api.GetRetweets(statusid=str(post), count=100)
                for retweet in retweets_post:
                    if retweet.user.id in self.dicRetweets.keys():
                        self.dicRetweets[retweet.user.id].append(
                            {'data': self.converter_formato_data(retweet.created_at),
                             'post': retweet.retweeted_status.id})

                horario_corrente = datetime.now()
                tempo_processamento = (horario_corrente - horario_coleta).total_seconds()
                {} if tempo_processamento > 12 else time.sleep(12 - tempo_processamento)
        except error.TwitterError as e:
            print("Erro durante coleta de retweets: " + str(e.message))

    def obter_posts_bot(self, bot, min_id):
        max_id = 9000000000000000000
        posts = []
        horario_coleta = None

        limite_posts = 0
        while True:
            try:
                horario_coleta = datetime.now()
                # Pega os posts do bot e armazena em lista
                timeline_bot = self.api.GetUserTimeline(user_id=bot, count=200, max_id=max_id, since_id=min_id,
                                                        exclude_replies=False, include_rts=False)
                for t in timeline_bot:
                    posts.append(t)
                    self.dicPosts[t.id] = self.converter_formato_data(t.created_at)

                if len(timeline_bot) > 0 and limite_posts == 0:
                    limite_posts = timeline_bot[0].id

            except error.TwitterError as e:
                print("Erro durante coleta de posts: " + str(e.message))

            # Termina a coleta se não houverem mais posts
            if len(timeline_bot) == 0:
                break
            else:
                # Define um novo limite para nova coleta
                if len(timeline_bot) > 0:
                    max_id = timeline_bot[len(timeline_bot) - 1].id - 1

            # Aguarda tempo para não exceder limite da API
            horario_corrente = datetime.now()
            tempo_processamento = (horario_corrente - horario_coleta).total_seconds()
            {} if tempo_processamento > 1 else time.sleep(1 - tempo_processamento)

        if limite_posts > 0:
            self.dicLimites['posts'] = limite_posts
        return posts

    @staticmethod
    def obter_bots():
        lines = []
        lines2 = []

        try:
            file = open("ArquivosEntrada/bots.txt", 'r')
            lines = file.readlines()
            print("Bots obtidos com sucesso")
            file.close()

            file2 = open("ArquivosSaida/botsColetados.txt", 'r')
            lines2 = file2.readlines()
            file2.close()
        except IOError as e:
            print("Erro ao obter os bots: " + str(e))

        bots_pendentes = []
        for t in lines:
            bot_coletado = False
            for a in lines2:
                bot_a = a.rstrip()
                bot_t = t.rstrip()
                if str(bot_a) == str(bot_t):
                    bot_coletado = True

            if bot_coletado is False:
                bots_pendentes.append(str(t).rstrip())

        return bots_pendentes

    # CRIAÇÃO DE ARQUIVOS DO BOT
    @staticmethod
    def criar_pasta_bot(bot):
        try:
            os.mkdir(path="ArquivosSaida/" + str(bot))
        except IOError as e:
            print("Erro ao criar pasta do bot: " + str(e))

    @staticmethod
    def primeira_execucao_feita():
        file = open("ArquivosEntrada/primeiraExecucao.txt", 'r')
        lines = file.readlines()
        file.close()

        for l in lines:
            if str(l.rstrip()) == "feito":
                return True
        return False

    @staticmethod
    def salva_primeira_execucao():
        file = open("ArquivosEntrada/primeiraExecucao.txt", "w")
        file.write("feito")
        file.close()

    def criar_arquivos_bot(self, bot, criacao_bot):
        try:
            path = "ArquivosSaida/" + str(bot) + "/criadoEm.txt"
            if not Path(path).is_file():
                criado_em = open(path, "w")
                criado_em.write("criadoEm\n")
                criado_em.write(str(self.converter_formato_data(criacao_bot)))
                criado_em.close()

            path = "ArquivosSaida/" + str(bot) + "/seguidores.txt"
            if not Path(path).is_file():
                seguidores = open(path, "w")
                seguidores.write("id\n")
                seguidores.close()

            path = "ArquivosSaida/" + str(bot) + "/respostas.txt"
            if not Path(path).is_file():
                respostas = open(path, "w")
                respostas.write("seguidor,idResposta,dataResposta\n")
                respostas.close()

            path = "ArquivosSaida/" + str(bot) + "/mencoes.txt"
            if not Path(path).is_file():
                mencoes = open(path, "w")
                mencoes.write("seguidor,idResposta,dataMencao\n")
                mencoes.close()

            path = "ArquivosSaida/" + str(bot) + "/limites.txt"
            if not Path(path).is_file():
                limites = open(path, "w")
                limites.write("tipoLimite,valor\n")
                limites.write("posts,0\ninteracoes,0\n")
                limites.close()

            path = "ArquivosSaida/" + str(bot) + "/posts.txt"
            if not Path(path).is_file():
                posts = open(path, "w")
                posts.close()

            path = "ArquivosSaida/" + str(bot) + "/seguidoresVinculacao.txt"
            if not Path(path).is_file():
                seguidores_vinculacao = open(path, "w")
                seguidores_vinculacao.write("idUsuario,ataVinculacao\n")
                seguidores_vinculacao.close()

            path = "ArquivosSaida/" + str(bot) + "/seguidoresDesvinculacao.txt"
            if not Path(path).is_file():
                seguidores_desvinculacao = open(path, "w")
                seguidores_desvinculacao.write("idUsuario,dataDesvinculacao\n")
                seguidores_desvinculacao.close()

        except IOError as e:
            print("Erro ao criar arquivos do bot:" + str(e))

    @staticmethod
    def bot_coletado(bot):
        try:
            arquivo = open("ArquivosSaida/botsColetados.txt", "a")
            arquivo.write(str(bot) + "\n")
            arquivo.close()
        except IOError as e:
            print("Erro ao salvar bot como coletado: " + str(e))

    @staticmethod
    def limpar_coletados():
        try:
            arquivo = open("ArquivosSaida/botsColetados.txt", "w")
            arquivo.write("")
            arquivo.close()
        except IOError as e:
            print("Erro ao limpar bots coletados: " + str(e))

    @staticmethod
    def converter_formato_data(data_twitter):
        created = data_twitter.split(" ")
        date_time = created[5] + "-" + str(strptime(created[1], '%b').tm_mon) + "-" + created[2] + " " + created[3]

        return date_time

    def valida_vinculacao_desvinculacao(self, bot, seguidores):
        seguidores_vinculados = self.obter_seguidor_vinculacao(bot)
        seguidores_desvinculados = self.obter_seguidor_desvinculacao(bot)

        seguidores = sorted(seguidores)
        seguidores_vinculados = sorted(seguidores_vinculados)
        seguidores_desvinculados = sorted(seguidores_desvinculados)

        # ADICIONA VÍNCULO A NOVOS SEGUIDORES DO ROBÔ
        horario_vinculacao = datetime.now()
        for seguidor in seguidores:
            if self.busca_binaria(seguidores_vinculados, seguidor) is False:
                self.dicSeguidoresVinculados[seguidor] = {'vinculacao': horario_vinculacao}

        # ADICIONA DESVÍNCULO A PESSOAS QUE DEIXARAM DE SEGUIR O ROBÔ
        horario_desvinculacao = datetime.now()
        for seg_vinculado in seguidores_vinculados:
            if self.busca_binaria(seguidores, seg_vinculado) is False:
                if self.busca_binaria(seguidores_desvinculados, seg_vinculado) is False:
                    self.dicSeguidoresDesvinculados[seg_vinculado] = {'desvinculacao': horario_desvinculacao}

    @staticmethod
    def busca_binaria(lista, valor):
        first = 0
        last = len(lista) - 1

        while first <= last:
            mid = (first + last) // 2
            if lista[mid] == valor:
                return True
            else:
                if valor < lista[mid]:
                    last = mid - 1
                else:
                    first = mid + 1
        return False

c = Coleta()
c.realizar_coleta()

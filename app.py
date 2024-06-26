from flask import Flask, request, render_template, send_file, redirect, url_for, after_this_request
import os
import pandas as pd
import numpy as np
import zipfile

app = Flask(__name__)
UPLOAD_FOLDER = 'uploads'
PARTITION_FOLDER = 'partitions'

# Garantir que os diretórios de upload e partição existam
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(PARTITION_FOLDER, exist_ok=True)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    arquivo_enviado = request.files.get('file')
    opcao_num_linhas = request.form.get('num_lines')
    num_linhas_custom = request.form.get('custom_num_lines')

    if opcao_num_linhas == 'custom':
        num_linhas = int(num_linhas_custom)
    else:
        num_linhas = int(opcao_num_linhas)

    if not arquivo_enviado or arquivo_enviado.filename == '':
        return redirect(url_for('index'))

    caminho_arquivo = salvar_arquivo_enviado(arquivo_enviado)
    try:
        particionar_csv(caminho_arquivo, num_linhas)
    except UnicodeDecodeError:
        return "Erro: Não foi possível decodificar o arquivo CSV. Por favor, verifique a codificação do arquivo."

    nome_zip = criar_zip()

    @after_this_request
    def limpeza(response):
        try:
            limpar_arquivos()
        except Exception as e:
            app.logger.error(f"Erro ao limpar os arquivos: {e}")
        return response

    return send_file(nome_zip, as_attachment=True)


def salvar_arquivo_enviado(file):
    """Salva o arquivo enviado na pasta de upload."""
    caminho_arquivo = os.path.join(UPLOAD_FOLDER, file.filename)
    file.save(caminho_arquivo)
    return caminho_arquivo


def particionar_csv(caminho_arquivo, num_linhas):
    try:
        df = pd.read_csv(caminho_arquivo, encoding='utf-8')
    except UnicodeDecodeError:
        df = pd.read_csv(caminho_arquivo, encoding='latin1')

    num_particoes = max(1, int(np.ceil(len(df) / num_linhas)))
    df_dividido = np.array_split(df, num_particoes)

    nome_base_arquivo = os.path.splitext(os.path.basename(caminho_arquivo))[0]
    for i, df_parte in enumerate(df_dividido):
        nome_arquivo_parte = f"{nome_base_arquivo}_parte_{i + 1}.csv"
        caminho_parte = os.path.join(PARTITION_FOLDER, nome_arquivo_parte)
        df_parte = pd.DataFrame(df_parte)  # Converter cada partição para um DataFrame
        df_parte.to_csv(caminho_parte, index=False)


def ler_csv_com_codificacao(caminho_arquivo):
    """Tenta ler o arquivo CSV com diferentes codificações."""
    try:
        return pd.read_csv(caminho_arquivo, encoding='utf-8')
    except UnicodeDecodeError:
        return pd.read_csv(caminho_arquivo, encoding='latin1')


def criar_zip():
    """Cria um arquivo zip contendo todos os CSVs particionados."""
    nome_zip = os.path.join(UPLOAD_FOLDER, 'particoes.zip')
    with zipfile.ZipFile(nome_zip, 'w') as zipf:
        for root, _, files in os.walk(PARTITION_FOLDER):
            for file in files:
                zipf.write(os.path.join(root, file), file)
    return nome_zip


def limpar_arquivos():
    """Limpa os arquivos CSV particionados e o arquivo zip."""
    # Limpar a pasta de partições
    for root, _, files in os.walk(PARTITION_FOLDER):
        for file in files:
            os.remove(os.path.join(root, file))

    # Limpar a pasta de uploads, incluindo o arquivo ZIP
    for root, _, files in os.walk(UPLOAD_FOLDER):
        for file in files:
            try:
                os.remove(os.path.join(root, file))
            except PermissionError as e:
                app.logger.error(f"PermissionError ao deletar o arquivo {file}: {e}")
            except Exception as e:
                app.logger.error(f"Erro ao deletar o arquivo {file}: {e}")


if __name__ == '__main__':
    app.run(debug=True)


import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Properties;
import java.util.UUID;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayHandler;
import org.apache.commons.lang.ArrayUtils;
import org.tmatesoft.svn.core.SVNDepth;
import org.tmatesoft.svn.core.SVNException;
import org.tmatesoft.svn.core.SVNLogEntry;
import org.tmatesoft.svn.core.SVNURL;
import org.tmatesoft.svn.core.io.SVNRepository;
import org.tmatesoft.svn.core.wc.ISVNStatusHandler;
import org.tmatesoft.svn.core.wc.SVNClientManager;
import org.tmatesoft.svn.core.wc.SVNDiffClient;
import org.tmatesoft.svn.core.wc.SVNRevision;
import org.tmatesoft.svn.core.wc.SVNStatus;

/**
 * Sincroniza o banco de dados referenciado em TestEntityManagerFactory.URL com as atualizações contidas no arquivo 'alteracoes-db.sql'.
 * @author raphael.pinheiro
 */
public class DatabaseSyncSVN {

  /**
   * Database execution will update the databasesync version without executing the DIFF in the database.
   */
  private static final boolean SKIP_SQL_DIFF = false;

  private static final int DB_URL = 1;

  private static final int DB_USER = 2;

  private static final int DB_PASSWD = 3;

  private static final int SVN_FILE_PATH = 4;

  /**
   * Version in which svn file is blank so databasesync can manage comparisons.
   */
  private static final Long SVN_FILE_INITIAL_BLANK_VERSION = 1417L;

  /**
   * Retorna o gerenciador de conexão com o SVN.
   * @return {@link SVNClientManager}
   */
  private SVNClientManager getClientManager() {
    return SVNClientManager.newInstance();
  }

  /**
   * Retorna uma conexão com banco de dados a partir dos parametros configurados em {@link TestEntityManagerFactory}.
   * @param url para conexão com banco
   * @param user usuário para conexão com banco
   * @param password senha para conexão com banco
   * @return {@link Connection}
   * @throws Exception erro obtendo conexão com banco de dados
   */
  public Connection getDatabaseConnection(final String url, final String user, final String password) throws Exception {
    Class.forName("org.postgresql.Driver");
    Properties props = new Properties();
    props.setProperty("user", user);
    props.setProperty("password", password);
    return DriverManager.getConnection(url, props);
  }

  /**
   * Busca na tabela de parametros de configuração 'ocn_parametro_configuracao' a versão atual do banco de dados.
   * @param conn conexão com banco de dados
   * @return versão atual do banco de dados
   * @throws Exception erro obtendo versão do banco de dados
   */
  public Long getDatabaseVersion(final Connection conn) throws Exception {
    QueryRunner run = new QueryRunner();
    Object[] result = run
        .query(conn,
            "SELECT valor from ocn_parametro_configuracao"
                + " WHERE id_grupo_parametro = (SELECT id_grupo_parametro from ocn_grupo_parametro WHERE nome_grupo = 'mfc') AND parametro = ?",
            new ArrayHandler(), "MFC_VERSAO_BD");
    if (result.length > 0) {
      return Long.parseLong(String.class.cast(result[0]));
    }
    throw new Exception("Versao do banco nao encontrada");
  }

  /**
   * Atualiza a versão do banco de dados na tabela 'ocn_parametro_configuracao'.
   * @param conn conexão com banco de dados
   * @param versaoNova versão atualizada
   * @throws Exception erro atualizando a versão do banco de dados
   */
  public void updateDatabaseVersion(final Connection conn, final Long versaoNova) throws Exception {
    QueryRunner run = new QueryRunner();
    int result = run.update(conn,
        "UPDATE ocn_parametro_configuracao SET valor = ?"
            + " WHERE id_grupo_parametro = (SELECT id_grupo_parametro from ocn_grupo_parametro WHERE nome_grupo = 'mfc') AND parametro = ?",
        versaoNova.toString(), "MFC_VERSAO_BD");

    if (result != 1) {
      throw new Exception("Versao do banco nao atualizada");
    }
  }

  /**
   * Busca as atualizações a serem executadas no banco de dados.
   * @param file arquivo que contém as atualizações
   * @param versaoInicial versão registrada no banco de dados
   * @param versaoFinal nova versão encontrada no svn
   * @param ignoreEOF indica se final do arquivo deve ignorar presença de \n
   * @return diferença entre a versão atual e a versão nova
   * @throws Exception erro obtendo alterações entre as versões
   */
  public String alteracoesArquivo(final File file, final Long versaoInicial, final Long versaoFinal, final boolean ignoreEOF) throws Exception {
    String diff = getDiffFile(file, -1, versaoInicial, versaoFinal);
    // alteracoes-db.sql deve ser comitado com uma quebra de linha no final. como a iteração acima apenda \n a cada linha lida a partir do diff
    // no final são esperadas no mínimo duas ocorrências de \n seguidas
    if (!ignoreEOF && !diff.replace(" ", "").endsWith("\n\n") && diff.length() > 0) {
      throw new Exception("Arquivo alteracoes-db.sql não termina com \\n. Por favor acrescente linha em branco no final.");
    }
    return diff;
  }

  /**
   * Retorna diff entre duas versões do svn.
   * @param file arquivo a ser comparado
   * @param n numero de linhas a serem consideradas (-1 = todas as linhas)
   * @param versaoInicial versão inicial
   * @param versaoFinal versão final
   * @return conteudo do comando diff.
   * @throws Exception erro retornando diff
   */
  private String getDiffFile(final File file, final int n, final Long versaoInicial, final Long versaoFinal) throws Exception {
    // Escreve arquivo a partir do diff do svn
    File tempFile = File.createTempFile("dbsync", UUID.randomUUID().toString());
    FileOutputStream fos = new FileOutputStream(tempFile);
    SVNDiffClient diffClient = getClientManager().getDiffClient();
    diffClient.doDiff(file, SVNRevision.HEAD, SVNRevision.create(versaoInicial), SVNRevision.create(versaoFinal), SVNDepth.INFINITY, true, fos, null);
    fos.close();

    StringBuilder sb = new StringBuilder();
    // Escreve script sql, retirando o sinal de + que aparece no primeiro caracter das linhas do diff
    // Assumimos que o arquivo será SEMPRE incremental, por isso ignoramos o sinal de - que eventualmente poderia aparecer no diff
    BufferedReader br = new BufferedReader(new FileReader(tempFile));
    int linhas = 0;
    while (br.ready() && (n == -1 || linhas < n)) {
      String line = br.readLine();
      if (line.startsWith("+") && !line.startsWith("+++")) {
        sb.append(line.substring(1));
        sb.append("\n");
        linhas++;
      }
    }
    br.close();
    return sb.toString();
  }

  /**
   * Busca as primeiras n linhas de uma determinada versão.
   * @param file arquivo que contém as atualizações
   * @param n numero de linhas retornadas
   * @param versao versão a ser considerada
   * @return diferença entre a versão atual e a versão nova
   * @throws Exception erro obtendo alterações entre as versões
   */
  public String obtemInicioArquivo(final File file, final int n, final Long versao) throws Exception {
    // Comparando com versão de arquivo em branco, o diff retornará o conteudo da versao passada por parametro
    Long versaoInicial = SVN_FILE_INITIAL_BLANK_VERSION;
    return getDiffFile(file, n, versaoInicial, versao);
  }

  /**
   * Obtém no svn a última versão do arquivo que contém as atualizações do banco de dados.
   * @param file arquivo com atualizações do banco
   * @return última versão encontrada no svn
   * @throws SVNException erro obtendo versão do arquivo no svn
   */
  public Long obterUltimaVersaoArquivo(final File file) throws SVNException {
    ISVNStatusHandler fakeHandler = new ISVNStatusHandler() {

      @Override
      public void handleStatus(final SVNStatus arg0) throws SVNException {

      }

    };

    // Busca desde a versão do banco até a última (-1)
    return getClientManager().getStatusClient().doStatus(file, SVNRevision.COMMITTED, SVNDepth.FILES, true, true, false, false, fakeHandler, new ArrayList<String>());
  }

  /**
   * Compara o início das duas versões para saber se são incrementais.
   * @param arquivo arquivo a comparar
   * @param versaoInicial versão inicial
   * @param versaoFinal versão final
   * @return versaoFinal é incrementa da versaoInicial
   * @throws Exception erro na comparação das versões
   */
  public boolean comparaHeaderVersoes(final File arquivo, final Long versaoInicial, final Long versaoFinal) throws Exception {
    // Numero de linhas utilizado para comparar inicio dos arquivos
    int linhas = 2;
    String sql = obtemInicioArquivo(arquivo, linhas, versaoInicial);
    if (sql == null) {
      throw new Exception(String.format("Erro buscando versao %d do arquivo %s", versaoInicial, arquivo.getName()));
    }
    String sql2 = obtemInicioArquivo(arquivo, linhas, versaoFinal);
    if (sql2 == null) {
      throw new Exception(String.format("Erro buscando versao %d do arquivo %s", versaoFinal, arquivo.getName()));
    }
    // Se as duas primeiras linhas são iguais, considera que o header é igual
    return sql.equals(sql2);
  }

  /**
   * Verifica em que versões o início do arquivo foi modificado.
   * @param arquivo arquivo a ser analisado
   * @param versaoInicial versao inicial
   * @param versaoFinal versao final
   * @return Long[] {ultima versão com o mesmo header da versão inicial, primeira versão com header diferente da versão inicial}
   * @throws Exception erro encontrando versões
   */
  @SuppressWarnings("unchecked")
  public Long[] encontraVersaoDeMudancaDeHeader(final File arquivo, final Long versaoInicial, final Long versaoFinal) throws Exception {
    int linhas = 2;
    SVNClientManager client = SVNClientManager.newInstance();
    // Busca desde a versão do banco até a última (-1)
    SVNURL url = client.getLogClient().getReposRoot(arquivo, null, SVNRevision.HEAD);
    SVNRepository repository = client.createRepository(url, true);
    Collection<SVNLogEntry> logEntries;

    logEntries = repository.log(new String[] {getClientManager().getStatusClient().doStatus(arquivo, true).getRepositoryRelativePath()}, null, versaoInicial + 1, versaoFinal, true,
        true);

    String sqlInicial = obtemInicioArquivo(arquivo, linhas, versaoInicial);
    if (sqlInicial == null) {
      throw new Exception(String.format("Erro buscando versao %d do arquivo %s", versaoFinal, arquivo.getName()));
    }

    long ultimoArquivoVersaoAntiga = versaoInicial;
    long primeiroArquivoVersaoNova = versaoFinal;
    for (SVNLogEntry entry : logEntries) {
      String sql = obtemInicioArquivo(arquivo, linhas, entry.getRevision());
      if (sql == null) {
        throw new Exception(String.format("Erro buscando versao %d do arquivo %s", versaoInicial, arquivo.getName()));
      }
      if (sql.equals(sqlInicial)) {
        ultimoArquivoVersaoAntiga = entry.getRevision();
      } else {
        // Encontrada versão na qual o header do arquivo foi alterado
        primeiroArquivoVersaoNova = entry.getRevision();
        break;
      }
    }
    return new Long[] {ultimoArquivoVersaoAntiga, primeiroArquivoVersaoNova};
  }

  /**
   * Executa as atualizações necessárias no banco de dados.
   * @param dbUrl url para conexão com banco
   * @param user usuário para conexão com banco
   * @param password senha para conexão com banco
   * @throws Exception erro executando atualizações
   */
  private void doAction(final String dbUrl, final String user, final String password, final String filePath) throws Exception {
    Connection conn = getDatabaseConnection(dbUrl, user, password);
    Statement stmt;
    try {
      File file = new File(filePath);
      Long versaoInicial = getDatabaseVersion(conn);
      log(String.format("Versao atual = [%d]", versaoInicial));
      Long versaoFinal = obterUltimaVersaoArquivo(file);
      log(String.format("Versao do arquivo %s = [%d]", file, versaoFinal));

      // Durante o release, a versão final aparece como -1. Ignorando este caso.
      if (versaoFinal < 0) {
        log("Versão final inválida. Parando a execução.");
        return;
      }

      conn.setAutoCommit(false);
      if (!SKIP_SQL_DIFF) {
        stmt = conn.createStatement();
        if (comparaHeaderVersoes(file, versaoInicial, versaoFinal)) {
          String sql = alteracoesArquivo(file, versaoInicial, versaoFinal, false);
          log(String.format("Alterações a executar:\n%s", sql));
          stmt.execute(sql);
        } else {
          boolean primeiraIteracao = true;
          while (!comparaHeaderVersoes(file, versaoInicial, versaoFinal)) {
            Long[] v = encontraVersaoDeMudancaDeHeader(file, versaoInicial, versaoFinal);
            if (primeiraIteracao) {
              if (!versaoInicial.equals(v[0])) {
                String sql = alteracoesArquivo(file, versaoInicial, v[0], true);
                log(String.format("Alterações a executar:\n%s", sql));
                stmt.execute(sql);
              }
              versaoInicial = v[1];
              primeiraIteracao = false;
            } else {
              String sql = obtemInicioArquivo(file, -1, v[0]);
              log(String.format("Alterações a executar:\n%s", sql));
              stmt.execute(sql);
              versaoInicial = v[1];
            }
          }
          String sql = obtemInicioArquivo(file, -1, versaoFinal);
          log(String.format("Alterações a executar:\n%s", sql));
          stmt.execute(sql);
        }
        DbUtils.closeQuietly(stmt);
      }

      updateDatabaseVersion(conn, versaoFinal);
      conn.commit();
      log(String.format("Banco atualizado corretamente para a versão [%d]", versaoFinal));

    } catch (Exception e) {
      conn.rollback();
      DbUtils.closeQuietly(conn);
      throw e;
    }
  }

  private static void log(final String msg, final Throwable... t) {
    System.out.println(msg);
    if (t != null)
      for (Throwable e : t)
        System.out.println(e.getMessage());
  }

  /**
   * Dispara a execução das atualizações do banco de dados.
   * @param args parametros de execução do main
   * @throws Exception erro executando atualizações
   */
  public static void main(final String[] args) throws Exception {
    try {
      if (ArrayUtils.isEmpty(args) || !"true".equals(args[0])) {
        DatabaseSyncSVN dbSync = new DatabaseSyncSVN();
        if (ArrayUtils.isNotEmpty(args) && args.length == 5) {
          log("Atualizando " + args[DB_URL]);
          dbSync.doAction(args[DB_URL], args[DB_USER], args[DB_PASSWD], args[SVN_FILE_PATH]);
        }
      } else {
        System.out.println("DatabaseSync skipped.");
      }
    } catch (Exception e) {
      log("Unexpected error!", e);
      e.printStackTrace();
    }
  }
}

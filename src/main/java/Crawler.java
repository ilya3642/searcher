import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;
import java.util.regex.Pattern;
import java.util.stream.Collectors;


public class Crawler {

    String[] filter = {"а", "вдобавок", "именно", "также", "то", "благодаря", "тому", "что", "благо", "того", "вследствие", "ещё", "еще", "вдобавок", "и", "но", "если", "бы",
            "затем", "зачем", "все", "всё", "чтобы", "зато", "именно", "поэтому", "притом", "все-таки", "следовательно", "тогда", "ибо", "вдобавок", "будто",
            "словно", "как", "кроме", "того", "либо", "лишь", "коли", "только", "столько", "сколько", "ни", "однако", "отчего", "тем", "пока", "отчего", "оттого",
            "покуда", "поскольку", "почему", "причем", "причём", "пусть", "более", "хотя", "чтоб", "чем"};
    private Connection conn = null; // соединение с БД в локальном файле


    /* 0. Конструктор Инициализация паука с параметрами БД */
    public Crawler(String fileName) throws SQLException {
        System.out.println("Конструктор");

        String db_url = "jdbc:sqlite:" + fileName; // формируемая строка для подключения к локальному файлу

        this.conn = DriverManager.getConnection(db_url);
        this.conn.setAutoCommit(true); // включить режим автоматической фиксации (commit) изменений БД
    }


    /* 0. Методы финализации, замена деструктора */
    protected void finalize() throws SQLException {
        conn.close(); // закрыть соединение
    }


    /* 1. Индексирование одной страницы   */
    public boolean addToIndex(String url, Document html_doc) {
        System.out.println("      addToIndex");
        if (!isIndexed(url)) {
            // проверить, была ли проиндексирован данных url   - isIndexed
            // если не был, то
            int urlId = getEntryId("urlList", "URL", url, true);
            //   получить тестовое содержимое страницы - getTextOnly
            String text = getTextOnly(html_doc);
            //   получить список отдельных слов  - separateWords
            long time = System.currentTimeMillis();
            List<String> wordList = separateWords(text);
            System.out.println(System.currentTimeMillis() - time + "  " + wordList.size());
            int location = 1, countToken = wordList.size();
            //   Для каждого найденного слова currentword в списке wordList[]
            //     получить id_слова для currentword  -  getEntryId(‘Таблица wordlist в БД’, ‘столбец word’, ‘currentword ’)
            //     внести данные id_слова + id_url + положение_слова в таблицу wordLocation
            StringBuilder progress;
            for (String currentWord : wordList) {
                int id = getEntryId("wordList", "word", currentWord, true);
                addLocationWord(id, urlId, location++);
                if (location % 50 == 0) {
                    progress = new StringBuilder();
                    for (int start = countToken / 20; start < countToken; start += countToken / 20)
                        if (start < location)
                            progress.append("=");
                        else progress.append(" ");
                    float now = (float) location / (float) countToken * 100;
                    System.out.print("Progress\t|" + progress.toString() + "|  " + String.format("%.02f", now) + "%\r");
                }
            }
            System.out.print("Progress\t|====================|  100%");
            System.out.println("\n\nComplete!\n");

            try {
                Statement statement = this.conn.createStatement();
                String sqlGetEntryId_SelectFromTable = "SELECT COUNT(*) FROM wordList;";

                ResultSet resultSet = statement.executeQuery(sqlGetEntryId_SelectFromTable);
                resultSet.next();
                System.out.println(resultSet.getInt(1));

            } catch (SQLException ex) {
                ex.printStackTrace();
            }
        }

        return true;
    }


    /* 2. Текстовое содержимое страницы   */
    public String getTextOnly(Document doc) {
        return doc.body().text();
    }


    /* 3. Разбиение текста на слова   */
    public List<String> separateWords(String text) {
        return Arrays.asList(Pattern.compile("\\s+").split(text.toLowerCase().replaceAll("\\,|\\.|\\?|\\!", "")));
    }


    /* 4. Проиндексирован ли URL   */
    public boolean isIndexed(String url) {
        // проверить, присутствует  ли url в БД  (Таблица urllist в БД)
        // проверить, присутствует  инф о найденных словах по адресу url (Таблица wordlocation в БД)
        int id = getEntryId("urlList", "URL", url, false);
        if (id == -1 ||
                getEntryId("wordLocation", "fk_URLId", Integer.toString(id), false) == -1)
            return false;
        else return true;
    }


    /* 5. Добавление ссылки с одной страницы на другую   */
    public void addLinkRef(String urlFrom, String urlTo, String linkText) {
        try {
            Statement statement = this.conn.createStatement();

            // добавить инф. в таблицу БД  linkbeetwenurl
            // добавить инф. в таблицу БД  linkwords
            String sqlCreateLinkBetweenRow = "INSERT INTO linkBeetwenURL (fk_FromURL_Id, fk_ToURL_Id) VALUES (" + urlFrom + ", " + urlTo + ");";
            statement.execute(sqlCreateLinkBetweenRow);

            addLinkWords(Integer.parseInt(urlTo), separateWords(linkText));

        } catch (SQLException ex) {
            ex.printStackTrace();
        }

    }

    public void addLinkWords(int urlId, List<String> linkWords) {
        try {
            Statement statement = this.conn.createStatement();
            for (String currentWord : linkWords) {
                int id = getEntryId("wordList", "word", currentWord, true);
                String sqlCreateLinkWordRow = "INSERT INTO linkWords (fk_wordId, fk_linkId) VALUES (" + id + ", " + urlId + ");";
                statement.execute(sqlCreateLinkWordRow);
            }
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }

    public void addLocationWord(int wordId, int urlId, int location) {
        try {
            Statement statement = this.conn.createStatement();

            String sqlCreateLocationRow = "INSERT INTO wordLocation (fk_wordId, fk_URLId, location) VALUES (" + wordId + ", " + urlId + ", " + location + ");";
            statement.execute(sqlCreateLocationRow);
        } catch (SQLException ex) {
            ex.printStackTrace();
        }
    }


    /* 7. Инициализация таблиц в БД   */
    public void createIndexTables() throws SQLException {
        System.out.println("Создание таблиц");

        // Получить Statement для того, чтобы выполнить sql-запрос
        Statement statement = this.conn.createStatement();


        // 1. Таблица wordList -----------------------------------------------------
        // Удалить таблицу wordList из БД
        String sqlWordList_DropTable = "DROP TABLE  IF EXISTS  wordList; ";
        System.out.println(sqlWordList_DropTable);
        statement.execute(sqlWordList_DropTable);

        // Создание таблицы wordList в БД
        // Сформировать SQL запрос
        String sqlWordList_CreateTable = "CREATE TABLE IF NOT EXISTS wordList ( \n "
                + "    rowid INTEGER PRIMARY KEY AUTOINCREMENT,  -- первичный ключ\n"
                + "    word      TEXT     NOT NULL, -- слово\n"
                + "    isFiltred INTEGER            -- флаг фильтрации\n"
                + ");";
        // Выполнить SQL запрос
        System.out.println(sqlWordList_CreateTable);
        statement.execute(sqlWordList_CreateTable);


        // 2. Таблица urlList -------------------------------------------------------
        // Удалить таблицу urlList из БД
        String sqlURLList_DropTable = "DROP TABLE  IF EXISTS  urlList; ";
        System.out.println(sqlURLList_DropTable);
        statement.execute(sqlURLList_DropTable);

        // Создания таблицы urlList в БД
        // Сформировать SQL запрос
        String sqlURLList_CreateTable = "CREATE TABLE IF NOT EXISTS urlList ( \n "
                + "    rowid INTEGER PRIMARY KEY AUTOINCREMENT,  -- первичный ключ\n"
                + "    url TEXT  -- адрес\n"
                + ");";
        // Выполнить SQL запрос
        System.out.println(sqlURLList_CreateTable);
        statement.execute(sqlURLList_CreateTable);


        // 3. Таблица wordLocation ---------------------------------------------------------------------
        // Удалить таблицу wordLocation из БД
        String sqlWordLocation_DropTable = "DROP TABLE  IF EXISTS  wordLocation; ";
        System.out.println(sqlWordLocation_DropTable);
        statement.execute(sqlWordLocation_DropTable);

        // Создания таблицы wordLocation в БД
        // Сформировать SQL запрос
        String sqlWordLocation_CreateTable = "CREATE TABLE IF NOT EXISTS wordLocation ( \n "
                + "    rowid INTEGER PRIMARY KEY AUTOINCREMENT,  -- первичный ключ\n"
                + "    fk_wordId INT NOT NULL, -- вторичный ключ - id слова\n"
                + "    fk_URLId INT NOT NULL, -- вторичный ключ - id URL\n"
                + "    location INT,  -- местоположение на странице\n"
                + "    FOREIGN KEY (fk_wordId) REFERENCES wordList (rowId) ON DELETE CASCADE ON UPDATE CASCADE,\n"
                + "    FOREIGN KEY (fk_URLId) REFERENCES urlList (rowId)\n ON DELETE CASCADE ON UPDATE CASCADE"
                + ");";
        // Выполнить SQL запрос
        System.out.println(sqlWordLocation_CreateTable);
        statement.execute(sqlWordLocation_CreateTable);


        // 4. Таблица linkBeetwenURL ---------------------------------------------------------------------
        // Удалить таблицу linkBeetwenURL из БД
        String sqlLinkBeetwenURL_DropTable = "DROP TABLE  IF EXISTS  linkBeetwenURL; ";
        System.out.println(sqlLinkBeetwenURL_DropTable);
        statement.execute(sqlLinkBeetwenURL_DropTable);

        // Создания таблицы linkBeetwenURL в БД
        // Сформировать SQL запрос
        String sqlLinkBeetwenURL_CreateTable = "CREATE TABLE IF NOT EXISTS linkBeetwenURL ( \n "
                + "    rowid INTEGER PRIMARY KEY AUTOINCREMENT,  -- первичный ключ\n"
                + "    fk_FromURL_Id INT NOT NULL, -- местоположение ссылки\n"
                + "    fk_ToURL_Id INT NOT NULL, -- сама ссылка\n"
                + "    FOREIGN KEY (fk_FromURL_Id) REFERENCES URLList (rowId) ON DELETE CASCADE ON UPDATE CASCADE ,\n"
                + "    FOREIGN KEY (fk_ToURL_Id) REFERENCES URLList (rowId) ON DELETE CASCADE ON UPDATE CASCADE"
                + ");";
        // Выполнить SQL запрос
        System.out.println(sqlLinkBeetwenURL_CreateTable);
        statement.execute(sqlLinkBeetwenURL_CreateTable);


        // 5. Таблица linkWords ---------------------------------------------------------------------
        // Удалить таблицу wordlocation из БД
        String sqlLinkwords_DropTable = "DROP TABLE  IF EXISTS  linkWords; ";
        System.out.println(sqlLinkwords_DropTable);
        statement.execute(sqlLinkwords_DropTable);

        // Создания таблицы linkWords в БД
        // Сформировать SQL запрос
        String sqllinkbeetwenurl_CreateTable = "CREATE TABLE IF NOT EXISTS linkWords ( \n "
                + "    rowid INTEGER PRIMARY KEY AUTOINCREMENT,  -- первичный ключ\n"
                + "    fk_wordId INT NOT NULL, -- id слова\n"
                + "    fk_linkId INT NOT NULL, -- id ссылки, частью которой он является\n"
                + "    FOREIGN KEY (fk_wordId) REFERENCES wordList (rowId) ON DELETE CASCADE ON UPDATE CASCADE ,\n"
                + "    FOREIGN KEY (fk_linkId) REFERENCES linkBetweenURL (rowId) ON DELETE CASCADE ON UPDATE CASCADE"
                + ");";
        // Выполнить SQL запрос
        System.out.println(sqllinkbeetwenurl_CreateTable);
        statement.execute(sqllinkbeetwenurl_CreateTable);
        // ...
    }


    /* 8. Вспомогательная функция для получения идентификатора и
      добавления записи, если такой еще нет */
    public int getEntryId(String table, String field, String value, boolean createNew) {
        try {
            Statement statement = this.conn.createStatement();
            String sqlGetEntryId_SelectFromTable = "SELECT rowId FROM " + table + " where " + field + " = '" + value + "';";

            ResultSet resultSet = statement.executeQuery(sqlGetEntryId_SelectFromTable);
            if (resultSet.next())
                return resultSet.getInt("rowId");
            else if (createNew) {
                String sqlCreateNewRow = "";
                if (table.equals("wordList")) {
                    String isFiltr = "0";
                    if (Arrays.asList(filter).contains(value))
                        isFiltr = "1";
                    sqlCreateNewRow = "INSERT INTO " + table + "(" + field + ",isFiltred)" + "VALUES ('" + value + "'," + isFiltr + ");";
                } else
                    sqlCreateNewRow = "INSERT INTO " + table + "(" + field + ")" + "VALUES ('" + value + "');";
                statement.execute(sqlCreateNewRow);
                return statement.executeQuery(sqlGetEntryId_SelectFromTable).getInt("rowId");
            }


        } catch (SQLException ex) {
            ex.printStackTrace();
        }

        return -1;
    }


    /* 6. Непосредственно сам метод сбора данных.
     * Начиная с заданного списка страниц, выполняет поиск в ширину
     * до заданной глубины, индексируя все встречающиеся по пути страницы   */

    public void crawl(ArrayList<String> urlList, int maxDepth) {
        System.out.println("Начало обхода всех страниц");

        // создать Множество(Set) очередных адресов  (уникальных - не повторяющихся)
        LinkedHashSet<String> nextUrlSet = new LinkedHashSet<String>();

        // для каждого уровня глубины currDept до максимального maxDepth
        for (int currDept = 0; currDept < maxDepth; currDept++) {
            System.out.printf("** Глубина  %d **************************************************************************\n",
                    currDept);


            int urlIndex = 0; // номер элемента в списке urlList

            // обход каждого url на теущей глубине
            for (int i = 0; i < urlList.size(); i++) {
                urlIndex = i;  // назначить номер элемента в списке urlList


                // шаг-1. Выбрать url-адрес для обработки
                String url = urlList.get(urlIndex); // получить url-адрес из списка
                Document html_doc = null;

                // блок контроля исключений при запросе содержимого страницы
                try {
                    Date date = new Date();
                    SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss");
                    System.out.printf("  %d/%d urlList[%d]- %s - Попытка открыть ", i + 1, urlList.size(), urlIndex, formatter.format(date));
                    // шаг-2. Запросить HTML-код
                    // шаг-3. Разобрать HTML-код на составляющие
                    html_doc = Jsoup.connect(url).get(); // получить структуру данных, которая содержит разобранные на элементы наполение html-страницы

                    // шаг-4. Найти и удалить на странице блоки со скриптами и стилями оформления ('script', 'style')
                    html_doc.select("script, noscript, style").remove();


                } catch (Exception e) {
                    // обработка исключений при ошибке запроса содержимого страницы
                    System.out.printf("     Ошибка. %s\n", url);
                    System.out.print(e);
                    continue; // перейти к следующему url
                }

                // если ошибок не было и содержимое страницы получено
                if (html_doc != null) {
                    System.out.printf("     Открыта %s\n", url);


                    // шаг-5. Добавить содержимого страницы в Индекс
                    addToIndex(url, html_doc);


                    // шаг-6. Извлечь с данный страницы инф о ссылка на внешние узлы = получить все тэги <a> = получить все ссылки

                    // Получить все теги <a>
                    Elements links = html_doc.getElementsByTag("a");

                    System.out.println("Wait until save all links with this page....\r");

                    // обработать каждый тег <a>
                    for (Element tagA : links) {

                        // получить содержимое аттрибута "href"
                        String nextUrl = tagA.attr("href");

                        // если ссылка не пустая и ее длина больше 6 символов
                        if (!nextUrl.equals("") && nextUrl.length() > 6) {

                            // Выбор "подходящих" ссылок => если ссылка начинается с "http"
                            if (nextUrl.substring(0, 4).equals("http")) {

                                if (nextUrl.substring(nextUrl.length() - 1).equals("/"))
                                    nextUrl = nextUrl.substring(0, nextUrl.length() - 1);

                                // добавить в Множество очередных ссылок nextUrlSet
                                nextUrlSet.add(nextUrl);

                                // добавить инф о ссылке в БД  -  addLinkRef(  url,  nextUrl)
                                addLinkRef(Integer.toString(getEntryId("URLList", "url", url, true)), Integer.toString(getEntryId("URLList", "url", nextUrl, true)), tagA.text());


                                //System.out.println("   ссылка    подходящая. добавить   " + nextUrl);
                            }

                        }
                    } //==== конец цикла для обработки тега <a>

                    System.out.println("Save complete!\n");

                }


                //====конец обработки одной ссылки url
            }
            //заменить содержимое URLlist на nextUrlSet
            urlList = new ArrayList<String>(nextUrlSet);

            //====конец обхода ссылкок URLlist на текущей глубине
        }
    }


    public static void main(String[] args) {

        Crawler myCrawler = null;
        try {
            myCrawler = new Crawler("test_regular.db");

            myCrawler.createIndexTables();

            ArrayList<String> pageList = new ArrayList<String>();
            pageList.add("https://habr.com/ru/news/t/543206/");

            myCrawler.crawl(pageList, 2);

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }
}
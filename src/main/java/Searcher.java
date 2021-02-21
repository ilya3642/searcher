import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.*;

public class Searcher {

    private Connection conn = null; // соединение с БД в локальном файле
    private int countWord = 0;

    /* 0. Конструктор  */
    public Searcher(String fileName) throws SQLException {
        String db_url = "jdbc:sqlite:" + fileName; // формируемая строка для подключения к локальному файлу
        this.conn = DriverManager.getConnection(db_url);
        this.conn.setAutoCommit(true); // включить режим автоматической фиксации (commit) изменений БД
    }

    /* 0. Метод финализации*/
    protected void finalize() throws SQLException {
        conn.close(); // закрыть соединение
    }

    public List<Integer> getWordsIds(String queryString) throws Exception {
        //Привести текст запроса к нижнему регистру
        queryString = queryString.toLowerCase();

        //Разделить поисковый запрос по пробелам на слова
        String[] queryWordsList = queryString.trim().split(" ");

        //список для хранения результата
        List<Integer> rowidList = new ArrayList<>();

        // Для каждого искомого слова
        for (String word :
                queryWordsList) {

            // Сформировать sql-запрос для получения rowid слова, указано ограничение на кол-во возвращаемых результатов (LIMIT 1)
            String sql = String.format("SELECT rowid FROM wordList WHERE word =\"%s\" LIMIT 1; ", word);
            //String sql2 = "SELECT * FROM wordlist limit 10;";
            // Выполнить sql-запрос. В качестве результата ожидаем строки содержащие целочисленный идентификатор rowid


            Statement statement = this.conn.createStatement();
            ResultSet resultRows = statement.executeQuery(sql);
            //ResultSet resultRows2 = this.conn.createStatement().executeQuery(sql2);

            while (resultRows.next()) {
                // Если слово было найдено и rowid получен
                // Искомое rowid является элементом строки ответа от БД (особенность получаемого результата)
                int word_rowid = resultRows.getInt("rowid");

                // поместить rowid в список результата
                rowidList.add(word_rowid);
                System.out.println("Поисковое слово :   " + word + ", его id = " + word_rowid);
            }

        }

        countWord = queryWordsList.length;

        if (rowidList.size() != queryWordsList.length)
            throw new SQLException("Одно из слов поискового запроса не найдено:\" + word");
        // вернуть список идентификаторов
        return rowidList;
    }


    /* 2. все сочетания позиций всех слов поискового запроса
     *   Поиск комбинаций из всезх искомых слов в проиндексированных url-адресах
     * :param queryString: поисковый запрос пользователя
     * :return: 1) список вхождений формата (urlId, loc_q1, loc_q2, ...) loc_qN позиция на странице Nго слова из поискового запроса  "q1 q2 ..."
     */
    public HashMap<Integer, String> getMatchRows(String queryString) throws SQLException, Exception {

        List<Integer> wordids = getWordsIds(queryString);
        //Разбиваем поисковый запрос на слова по пробелам

        // получить идентификаторы искомых слов

        StringBuilder sqlFullQuery = new StringBuilder();

        //Созать переменную для полного SQL-запроса

        // Созать объекты-списки для дополнений SQL-запроса
        List<String> sqlpart_Name = new ArrayList<>();//имена столбцов
        List<String> sqlpart_Join = new ArrayList<>();//INNER JOIN
        List<String> sqlpart_Condition = new ArrayList<>();//условия WHERE


        //Конструктор SQL-запроса (заполнение обязательной и дополнительных частей)
        //обход в цикле каждого искомого слова и добавлене в SQL-запрос соответствующих частей
        for (int wordIndex = 0; wordIndex < wordids.size(); wordIndex++) {
            int wordId = wordids.get(wordIndex);

            if (wordIndex == 0) {

                sqlpart_Name.add(" w0.fk_URLId ");
                sqlpart_Name.add(", w0.location ");
                sqlpart_Condition.add(String.format("WHERE w0.fk_wordId=%s ", wordId));

            } else {

                sqlpart_Name.add(String.format(", w%s.location ", wordIndex));
                sqlpart_Join.add(String.format(" INNER JOIN wordLocation w%s on w0.fk_URLId=w%s.fk_URLId ", wordIndex, wordIndex));
                sqlpart_Condition.add(String.format(" AND w%s.fk_wordId=%s ", wordIndex, wordId));
            }


            // Получить идентификатор слова

            //if wordIndex ==0:
            // обязательная часть для первого слова

            //else:
            // Дополнительная часть для 2,3,.. искомых слов

            // Проверка, если текущее слово - второе и более
            // Добавить в имена столбцов
            //Добавить в sql INNER JOIN
            // Добавить в sql ограничивающее условие

        }


        // Объеднение запроса из отдельных частей
        //Команда SELECT

        sqlFullQuery.append("SELECT");

        //Все имена столбцов для вывода
        sqlpart_Name.forEach((sqlPart) -> {
            sqlFullQuery.append("\n");
            sqlFullQuery.append(sqlPart);
        });

        // обязательная часть таблица-источник
        sqlFullQuery.append("\nFROM wordLocation w0 ");

        //часть для объединения таблицы INNER JOIN

        sqlpart_Join.forEach((sqlPart) -> {
            sqlFullQuery.append("\n");
            sqlFullQuery.append(sqlPart);
        });

        //обязательная часть и дополнения для блока WHERE

        sqlpart_Condition.forEach((sqlPart) -> {
            sqlFullQuery.append("\n");
            sqlFullQuery.append(sqlPart);
        });

        System.out.println(sqlFullQuery);

        // Выполнить SQL-запроса и извлеч ответ от БД

        Statement statement = this.conn.createStatement();
        ResultSet resultRows = statement.executeQuery(sqlFullQuery.toString());

        //Для каждого слова получаем его идентификатор(rowid) в wordlist
        //Выбираем те ссылки links[], для которых есть связи со всеми
        // словами в запросе

        System.out.println("Количество слов в поисковом запросе :" + wordids.size());
        HashMap<Integer, String> wordCombinations = new HashMap<>();

        int wordIndex;
        int rowId = 0;
        StringBuilder stringBuilder = new StringBuilder();

        while (resultRows.next()) {

            if (rowId != resultRows.getInt(1)) {
                if (rowId != 0) {
                    wordCombinations.put(rowId, stringBuilder.toString());
                    stringBuilder = new StringBuilder();
                }
                rowId = resultRows.getInt(1);
            }

            wordIndex = 1;

            while (wordIndex <= wordids.size()) {
                stringBuilder.append("/").append(resultRows.getInt(wordIndex + 1));
                wordIndex++;
            }
            stringBuilder.append(" ");


        }

        if (wordCombinations.size() == 0)
            wordCombinations.put(rowId, stringBuilder.toString());


        return wordCombinations;
    }

    /*  3. Функция приводит значение ранга к диапазону 0...1, где 0-"хуже", 1 - "лучше".
     :param scores:  словарь идентификаторов и рангов
     :param smallIsBetter: режим чтения входного ранга. лучше – меньший или больший
     :return: возвращает новый словарь ранг находится в диапазоне от 0 до 1,  1 - "лучше". */
    public HashMap normalizationScores(HashMap<Integer, Float> scores, boolean smallIsBetter) {

        HashMap<Integer, Float> normalizeScores = new HashMap<>();
        // создать словарь с результатом

        // создать переменную vsmall - малая величина, вместо деления на 0
        int vsmall = 0;

        float min = 10000;
        float max = 0;

        Set urlIds = scores.keySet();

        for (Object url : urlIds) {
            float value = scores.get(url);

            // получить максимум
            if (value > max)
                max = value;

            // получить минимум
            if (value < min)
                min = value;
        }

        // перебор каждой пары ключ значение
        // Режим МЕНЬШЕ вх. значение => ЛУЧШЕ
        if (smallIsBetter) {
            // ранг нормализованный = мин. / (тек.значение или малую величину)
            for (Object url : urlIds) {
                float value = min / (scores.get(url));
                normalizeScores.put((int) url, value);
            }
        }
        // Режим БОЛЬШЕ  вх. значение => ЛУЧШЕ вычислить макс и разделить каждое на макс
        else {
            // вычисление ранга как доли от макс.
            // ранг нормализованный = тек. значения / макс.
            for (Object url : urlIds) {
                float value = (scores.get(url)) / max;
                normalizeScores.put((int) url, value);
            }
        }


        return normalizeScores;
    }

    /* 4.  Метрика по содержимому Расстояние между искомыми словами */
    public HashMap distanceScore(HashMap rows, int countWord) {
        // Создать mindistanceDict - словарь с дистанций между словами внутри комбинаций искомых слов

        HashMap<Integer, Float> minDistanceDict = new HashMap();

        System.out.println("Количество уникальных url-адресов, содержащих слова поискового запроса : " + rows.size());

        // Если есть только одно слово, все ранги всех url-адресов будут равны
        if (countWord == 1)
            // поместить в словарь mindistanceDict все ключи urlid с начальным значением сумм дистанций между словами комбинаций "1.0"
            rows.keySet().forEach((urlId) -> minDistanceDict.put((Integer) urlId, 1f));

            // Иначе искомых слов два и больше провести вычисления
            // поместить в словарь mindistanceDict все ключи urlid с начальным большими значениями 1000000
        else {
            rows.keySet().forEach((urlId) -> {
                minDistanceDict.put((Integer) urlId, 1000000f);


                String[] wordCombinations = ((String) rows.get(urlId)).split(" ");

                // Цикл обхода каждой строки-комбинации искомых слов
                for (String combination : wordCombinations) {


                    String[] locations = combination.trim().replaceFirst("/", "").split("/");

                    int[] numLoc = new int[locations.length];

                    System.out.print("urlid = " + urlId + ": ");
                    for (int i = 0; i < locations.length; i++) {
                        numLoc[i] = Integer.parseInt(locations[i]);
                        System.out.print("loc" + (i + 1) + " = " + numLoc[i] + ", ");
                    }

                    System.out.println();


                    // накапливаемая сумма дистанций для данной комбинации искомых слов
                    float distance = 0;

                    // Цикл обхода каждой пары положений соседних искомых слов (loc_q1, loc_q2, ..)
                    for (int i = 0; i < numLoc.length - 1; i++)
                        //вычислить дистанцию между парой соседнего  abs(loc_q1-loc_q2)
                        // и суммировать все вычисленные дистанции для данной комбинации
                        distance += Math.abs(numLoc[i] - numLoc[i + 1]);

                    // Проверка, является дистанция между словами в найденной комбинации мешьше, чем предыдущие для данной urlid
                    if (minDistanceDict.get(urlId) > distance)
                        minDistanceDict.put((int) urlId, distance);
                }
            });
        }

        return minDistanceDict;
    }


    /* 5. получение текстового поля url-адреса по указанному urlid.  */
    public String getUrlName(int urlid) throws SQLException {

        // сформировать SQL-запрос вида SELECT url FROM urllist WHERE rowid=
        String queryGet = String.format("SELECT url FROM urlList WHERE rowid = %s", urlid);


        // выполнить запрос в БД
        Statement statement = this.conn.createStatement();
        ResultSet resultSet = statement.executeQuery(queryGet);

        // извлечь результат - строковый url и вернуть его
        resultSet.next();


        return resultSet.getString(1);
    }


    public HashMap pageRank(int iterations) throws SQLException {

        Statement statement = this.conn.createStatement();

        statement.execute("DROP TABLE IF EXISTS pagerank");
        statement.execute("CREATE TABLE IF NOT EXISTS  pagerank(" +
                "rowid INTEGER PRIMARY KEY AUTOINCREMENT," +
                "urlid INTEGER," +
                "score REAL);");


        statement.execute("DROP INDEX IF EXISTS wordidx;");
        statement.execute("DROP INDEX IF EXISTS urlidx;");
        statement.execute("DROP INDEX IF EXISTS wordurlidx;");
        statement.execute("DROP INDEX IF EXISTS urltoidx;");
        statement.execute("DROP INDEX IF EXISTS urlfromidx;");


        statement.execute("CREATE INDEX IF NOT EXISTS wordidx ON wordList(word);");
        statement.execute("CREATE INDEX IF NOT EXISTS urlidx ON urlList(url);");
        statement.execute("CREATE INDEX IF NOT EXISTS wordurlidx ON wordLocation(fk_wordId);");
        statement.execute("CREATE INDEX IF NOT EXISTS urltoidx ON linkBeetwenURL(fk_ToURL_Id);");
        statement.execute("CREATE INDEX IF NOT EXISTS urlfromidx ON linkBeetwenURL(fk_FromURL_Id);");

        statement.execute("DROP INDEX IF EXISTS rankurlididx;");
        statement.execute("CREATE INDEX IF NOT EXISTS rankurlididx ON pagerank(urlid);");

        statement.execute("REINDEX wordidx;");
        statement.execute("REINDEX urlidx;");
        statement.execute("REINDEX wordurlidx;");
        statement.execute("REINDEX urltoidx;");
        statement.execute("REINDEX urlfromidx;");
        statement.execute("REINDEX rankurlididx;");

        statement.execute("INSERT INTO pagerank (urlid, score) SELECT  rowid, 1.0 FROM urlList;");


        for (int i = 0; i < iterations; i++) {
            Statement statement1 = this.conn.createStatement();
            ResultSet resultSet = statement1.executeQuery("SELECT rowid FROM urlList;");
            while (resultSet.next()) {
                float pr = 0.15f;
                int urlid = resultSet.getInt(1);
                Statement statement2 = this.conn.createStatement();
                ResultSet setRefUrl = statement2.executeQuery(String.format("SELECT DISTINCT fk_FromURL_Id FROM linkBeetwenURL WHERE fk_ToURL_Id = %s;", urlid));
                while (setRefUrl.next()) {
                    Statement statement3 = this.conn.createStatement();
                    ResultSet countOutRef = statement3.executeQuery(String.format("SELECT COUNT(*) FROM linkBeetwenURL WHERE fk_FromURL_Id = %s;", setRefUrl.getInt(1)));

                    Statement statement4 = this.conn.createStatement();
                    ResultSet setRank = statement4.executeQuery(String.format("SELECT score FROM pagerank WHERE urlid = %s;", setRefUrl.getInt(1)));
                    setRank.next();
                    pr += (setRank.getFloat(1)) / countOutRef.getInt(1);
                }
                System.out.println(urlid);
                Statement statement5 = this.conn.createStatement();
                statement5.execute(String.format(Locale.US, "UPDATE pagerank SET score = %.2f WHERE urlid = %d;", pr, urlid));
            }
        }

        HashMap<Integer, Float> pageRanks = new HashMap();

        statement = this.conn.createStatement();
        ResultSet allPageRank = statement.executeQuery("SELECT urlid, score FROM pagerank;");
        while (allPageRank.next())
            pageRanks.put(allPageRank.getInt(1), allPageRank.getFloat(2));

        return pageRanks;

    }

    /*  6.  На поисковый запрос формирует список URL, вычисляет ранги, выводит в отсортированном порядке*/
    public void getSortedList(String queryString) throws Exception {


        // получить rowsLoc и wordids от getMatchRows(queryString)
        // rowsLoc - Список вхождений: urlId, loc_q1, loc_q2, .. слов из поискового запроса "q1 q2 ..."
        // wordids - Список wordids.rowid слов поискового запроса
        HashMap<Integer, String> wordCombinations = getMatchRows(queryString);


        HashMap<Integer, Float> urlScore = distanceScore(wordCombinations, countWord);


        // Получить m1Scores - словарь {id URL страниц где встретились искомые слова: вычисленный нормализованный РАНГ}
        // как результат вычисления одной из метрик
        urlScore = normalizationScores(urlScore, true);

        //Создать список для последующей сортировки рангов и url-адресов
        List<Integer> urlId = new ArrayList(urlScore.keySet());

        HashMap<Integer, Float> pageRank = normalizationScores(pageRank(3), false);

        HashMap<Integer, Float> m3Score = new HashMap();

        for (int id : urlId) {
            m3Score.put(id, (urlScore.get(id) + pageRank.get(id)) / 2);
            System.out.println(id + "   " + urlScore.get(id) + "   " + pageRank.get(id) + "   " + (urlScore.get(id) + pageRank.get(id)) / 2);
        }

        urlId = new ArrayList();

        List score = new ArrayList(m3Score.values());

        Collections.sort(score, Collections.reverseOrder());

        for (Object sc : score) {
            for (Map.Entry<Integer, Float> pair : m3Score.entrySet())
                if (pair.getValue() == sc) {
                    urlId.add(pair.getKey());
                    break;
                }
        }

        for (int id : urlId) {
            System.out.println(String.format(Locale.US, "urlId = %d | M1 = %.5f | M2 = %.5f | M3 = %.5f | urlText = %s", id, urlScore.get(id), pageRank.get(id), m3Score.get(id), getUrlName(id)));
        }

        int[] id;
        if (urlId.size() > 4)
            id = new int[4];
        else id = new int[urlId.size()];

        for (int i = 0; i < id.length; i++) {
            id[i] = urlId.get(i);
        }


        getHTML(queryString.trim().toLowerCase().split(" "), id);


    }

    public void createMarkedHTMLFile(String[] queryWords, String[] words, String fileName) {


        PrintWriter writer = null;


        StringBuilder htmlCode = new StringBuilder("<html><body><b>");

        int countNewLine = 0;
        for (String word : words) {
            String color = "";
            if (Arrays.asList(queryWords).contains(word)) {
                if (word.equals(queryWords[0]))
                    color = "red";
                else
                    color = "green";
                htmlCode.append("<span style=\"background-color:").append(color).append(";\">").append(word).append(" </span>");

            } else
                color = "white";
            htmlCode.append(word).append(" ");
            if (countNewLine++ > 100) {
                htmlCode.append("\n");
                countNewLine = 0;
            }
        }
        htmlCode.append("</b></body></html>");


        try {
            writer = new PrintWriter(fileName, "UTF-8"); // тут путь куда мы сохраняем наш html файл
            writer.println(htmlCode);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } finally {
            writer.close();


        }
    }

    public void getHTML(String[] queryWords, int[] urlid) throws SQLException {

        for (int i = 0; i < urlid.length; i++) {
            Statement statement = this.conn.createStatement();
            ResultSet countWord = statement.executeQuery(String.format("SELECT COUNT(*) FROM wordList w INNER JOIN wordLocation loc on loc.fk_wordId=w.rowid WHERE loc.fk_URLId = %d;", urlid[i]));
            countWord.next();
            String[] words = new String[countWord.getInt(1)];
            statement = this.conn.createStatement();
            ResultSet setWords = statement.executeQuery(String.format("SELECT word FROM wordList w INNER JOIN wordLocation loc on loc.fk_wordId=w.rowid WHERE loc.fk_URLId = %d;", urlid[i]));
            int j = 0;
            while (setWords.next()) {
                words[j] = setWords.getString(1);
                j++;
            }

            createMarkedHTMLFile(queryWords, words, Integer.toString(i + 1) + ".html");
        }


    }


    public static void main(String[] args) {

        Searcher mySearcher = null;
        //блок контроля исключительных ситуаций
        try {
            //создать экземпляр класса rgr.Searcher, с указанием имени файла БД
            mySearcher = new Searcher("javaDB_RGR.db");

            //Сформировать поисковый запрос
            String mySearchQuery = "хэтчбек автомат";

            //Получить все url-адреса содржащие искомые слова
            mySearcher.getSortedList(mySearchQuery);

        } catch (Exception throwables) {
            //при возникновении исключительной ситуации вывести цепочку вызовов функций
            throwables.printStackTrace();
        }
    }

}

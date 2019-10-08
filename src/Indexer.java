import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.simple.JSONArray;
import java.io.*;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.HashMap;


public class Indexer {


    public static JSONArray readJson(String file) throws IOException, ParseException {

        JSONParser parser = new JSONParser();
        FileReader fileReader = new FileReader(file);
        Object obj = parser.parse(fileReader);
        return (JSONArray) obj;
    }



    public static HashMap<String, Integer> UniqueWords(String str) {

        Pattern p = Pattern.compile("[a-zA-Z]+");
        Matcher m = p.matcher(str);
        HashMap<String, Integer> hm = new HashMap<>();

        while (m.find()) {
            String word = m.group();
            if(!hm.containsKey(word))
                hm.put(word, 1);
            else
                hm.put(word, hm.get(word) + 1);
        }
        return hm;
    }


    public static void main(String[] args) throws IOException, ParseException {

        JSONArray jsonarray = readJson("test.json");
        for (Object item : jsonarray) {
            JSONObject i = (JSONObject) item;
            String text = i.get("text").toString();

            HashMap<String, Integer> result = UniqueWords(text);
            for (Object o : result.entrySet()) {
                Map.Entry mapElement = (Map.Entry) o;
                int freq = ((int) mapElement.getValue());
                System.out.println(mapElement.getKey() + " : " + freq);
            }
//            break;
        }
    }
}

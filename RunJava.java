import java.io.*;
import java.net.*;
public class RunJava {
  public static void main(String args[]){
   String s = null;
   try{
    String query = "select distinct * where {  " +
           "?writer <http://dbpedia.org/ontology/nationality> ?nationality . " + 
           " ?writer <http://xmlns.com/foaf/0.1/depiction> ?depiction. " +
           " ?writer <http://dbpedia.org/ontology/deathDate> ?deathdate . " +
           " ?writer <http://dbpedia.org/ontology/notableWork> ?notablework. " +
           " ?writer <http://dbpedia.org/ontology/birthDate> ?birthdate . " +
           " ?writer <http://dbpedia.org/ontology/genre> ?genre . " +
           " ?writer <http://dbpedia.org/ontology/birthPlace> ?birthplace . " +
           " ?writer <http://xmlns.com/foaf/0.1/name> ?name . " +
           " ?writer <http://dbpedia.org/ontology/award> ?award . " +
           " ?writer <http://dbpedia.org/ontology/occupation> ?occupation . " +
           " } LIMIT 100";
    System.out.println(query);
   
    Process p = Runtime.getRuntime().exec("python2.7 runMulder.py -c config/config.json -q " + URLEncoder.encode(query));

    BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
    BufferedReader br_err = new BufferedReader(new InputStreamReader(p.getErrorStream()));
   
    System.out.println("Results retured from MULDER:");
    while ((s=br.readLine()) != null && !s.equals("EOF")){
	//put s in your hashlist or other data structure
	System.out.println(s);
     }

    System.exit(0);

  }catch (Exception e){System.out.println("Exception happened"); e.printStackTrace();}

 }

}

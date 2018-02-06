package ifmo.jackalope;

import com.tuneit.jackalope.dict.wiki.engine.core.Gloss;
import com.tuneit.jackalope.dict.wiki.engine.core.SenseOption;
import com.tuneit.jackalope.dict.wiki.engine.core.SenseOptionType;
import com.tuneit.jackalope.dict.wiki.engine.core.WikiSense;
import com.tuneit.jackalope.dict.wiki.engine.utils.WiktionarySnapshot;
import org.apache.commons.lang3.time.StopWatch;
import org.neo4j.driver.v1.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.neo4j.driver.v1.Values.parameters;

public class App implements AutoCloseable {

    private final Driver driver;

    public App(String uri, String user, String password) {
        driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
    }

    @Override
    public void close() {
        driver.close();
    }

    public static void main(String... args) {
        if (args.length == 0) {
            System.out.println("Provide first argument - path to snapshot!");
            System.exit(1);
        }
        try (App app = new App("bolt://localhost:7687", "admin", "admin")) {
            try (Session session = app.driver.session()) {
                SnapshotLoader wiki = new SnapshotLoader(args[0]);
                StopWatch watch = StopWatch.createStarted();
                Collection<WikiSense> senses = wiki.get_senses();
                System.out.println(String.format("Export to neo4j started. %d nodes to export.", senses.size()));
                int count = 0, divider = 1000;
                for (WikiSense sense : senses) {
                    app.export_sense_options_loop(session, sense, wiki.getSnapshot());
                    count++;
                    if (count % divider == 0) {
                        System.out.println(String.format("%d senses exported %s.", count, watch));
                    }
                }
                watch.stop();
                System.out.println(String.format("Export %d nodes from %d took %s", count, senses.size(), watch));
            }
        }
    }

    private void export_sense_options_loop(Session session, WikiSense sense, WiktionarySnapshot snapshot) {
        Gloss gloss = sense.getObjectGloss();
        /* lemma to sense */
        session.writeTransaction(tx -> tx.run(
                "MERGE (l: Lemma {name: $lemma_name})\n" +
                "MERGE (s: Sense {name: $sense_name, gloss_text: $gloss_text})\n" +
                "CREATE (l)-[r:HAS_SENSE]->(s)",
                parameters("lemma_name", sense.getLemma(),
                        "sense_name", sense.getNamedId(),
                        "gloss_text", gloss.getGlossText())
        ));
        /* sense to lemma option */
        for (SenseOption option : sense.getAllOptions()) {
            session.writeTransaction(tx -> tx.run(
                    "MERGE (l: Lemma {name: $option})\n" +
                    "MERGE (s: Sense {name: $sense_name, gloss_text: $gloss_text})\n" +
                    "CREATE (s)-[r:" + option.getType().name() + "]->(l)\n",
                    parameters("sense_name", sense.getNamedId(),
                            "gloss_text", gloss.getGlossText(),
                            "option", option.getOption().toString())
            ));
        }
        /* sense to sense link */
        for (Map.Entry<String, SenseOptionType> entry : sense.getLinks().entrySet()) {
            String wiki_id = entry.getKey();
            WikiSense target_sense = snapshot.getSenses().get(wiki_id);
            SenseOptionType option_type = entry.getValue();
            session.writeTransaction(tx -> tx.run(
                    "MERGE (source: Sense {name: $source_name, gloss_text: $source_gloss_text})\n" +
                    "MERGE (target: Sense {name: $target_name, gloss_text: $target_gloss_text})\n" +
                    "CREATE (source)-[r:" + option_type.name() + "]->(target)\n",
                    parameters("source_name", sense.getNamedId(),
                            "source_gloss_text", sense.getObjectGloss().getGlossText(),
                            "target_name", target_sense.getNamedId(),
                            "target_gloss_text", target_sense.getObjectGloss().getGlossText())
            ));
        }
    }

    private void export_sense_with_opetions_in_list(Session session, WikiSense sense) {
        Gloss gloss = sense.getObjectGloss();
        List<ArrayList> options = new ArrayList<>();
        for (SenseOption option : sense.getAllOptions()) {
            ArrayList<String> type_to_option = new ArrayList<>();
            type_to_option.add(option.getType().name());
            type_to_option.add(option.getOption().toString());
            options.add(type_to_option);
        }
        session.writeTransaction(tx -> tx.run(
               "MERGE (l: Lemma {name: $lemma_name})\n" +
               "CREATE (s: Sense {name: $sense_name, gloss_text: $gloss_text})\n" +
               "CREATE (l)-[r:HAS_SENSE]->(s)\n" +
               "FOREACH (type_to_option IN $options | CREATE (s)-[:OPTION{name:type_to_option[0]}]->(op:Lemma {name: type_to_option[1]})) ",
               parameters("lemma_name", sense.getLemma(),
                       "sense_name", sense.getNamedId(),
                       "gloss_text", gloss.getGlossText(),
                       "options", options)
        ));
    }

}

package ifmo.jackalope;

import com.tuneit.jackalope.dict.wiki.engine.api.EngineContext;
import com.tuneit.jackalope.dict.wiki.engine.api.WikiEngine;
import com.tuneit.jackalope.dict.wiki.engine.api.WikiEngineImpl;
import com.tuneit.jackalope.dict.wiki.engine.core.WikiSense;
import com.tuneit.jackalope.dict.wiki.engine.core.ru.RuEngineContext;
import com.tuneit.jackalope.dict.wiki.engine.utils.WiktionarySnapshot;
import com.tuneit.jackalope.dict.wiki.engine.utils.WiktionarySnapshotManager;
import org.apache.commons.lang3.time.StopWatch;

import java.util.ArrayList;
import java.util.Collection;

public class SnapshotLoader {
    private final WiktionarySnapshot snapshot;

    SnapshotLoader(String PATH_TO_SNAP) {
        EngineContext ruWikiEngineContext = new RuEngineContext();
        WikiEngine ruWikiEngine = new WikiEngineImpl(ruWikiEngineContext);
        WiktionarySnapshotManager snapshotManager = new WiktionarySnapshotManager(ruWikiEngine);
        System.out.println("Opening snapshot");
        StopWatch watch = StopWatch.createStarted();
        this.snapshot = snapshotManager.openSnapshot(PATH_TO_SNAP);
        watch.stop();
        System.out.println("Snapshot opened for " + watch);
    }

    public WiktionarySnapshot getSnapshot(){
        return snapshot;
    }

    public static void main(String[] args) {
        SnapshotLoader test = new SnapshotLoader(args[0]);
        Collection<WikiSense> senses = test.get_senses();
        int i = 0;
        for (WikiSense sense : senses) {
            if (!sense.getLinks().isEmpty()) {
                System.out.println(sense);
            } else {
                i++;
            }
        }
        System.out.println(senses.size() + " " + i);
    }

    WikiSense get_test_sense() {
        ArrayList<WikiSense> senses = new ArrayList<>(this.snapshot.getSenses().values());
        return senses.get(1);
    }

    Collection<WikiSense> get_senses() {
        return this.snapshot.getSenses().values();
    }
}

import com.tradeshift.blayze.Model
import com.tradeshift.blayze.dto.Inputs
import com.tradeshift.blayze.dto.Update
import com.tradeshift.blayze.features.Multinomial
import com.tradeshift.blayze.features.Text
import org.junit.Assert
import org.junit.Test
import kotlin.streams.toList

class ProductClassificationTest {

    @Test
    fun can_fit_20newsgroup() {
        val train = productClassification("train.csv")
        val model = Model(textFeatures = mapOf("q" to Text(pseudoCount = 0.001, includeFeatureProbability = 1.0))).batchAdd(train)

        println("training finished")
        val test = productClassification("test.csv")
        val acc = test
                .parallelStream()
                .map {
                    if (it.outcome == model.predict(it.inputs).maxBy { it.value }?.key) {
                        1.0
                    } else {
                        0.0
                    }
                }
                .toList()
                .average()
        //println(model.predict(Inputs(mapOf("q" to "gloves"))).maxBy { it.value })
//        println(model.predict(Inputs(mapOf("q" to "CDEFECBFCCE"))).maxBy { it.value })
//        println(model.predict(Inputs(mapOf("q" to "abcdduvytrbsjhcgsbfgswofyvzojdnfys"))).maxBy { it.value })
//        println(model.predict(Inputs(mapOf("q" to "mittens"))).maxBy { it.value })
//        println(model.predict(Inputs(mapOf("q" to "mittenx"))).maxBy { it.value })
//        println(model.predict(Inputs(mapOf("q" to "glof"))).maxBy { it.value })

        //println(model.predict(Inputs(mapOf("q" to "asfdasdfasdfasdfasfdadfafd"))).maxBy { it.value })
        println(acc)
        //Assert.assertTrue(acc > 0.65) // sklearn MultinomialNB with a CountVectorizer gets ~0.646
    }

    fun productClassification(fname: String): List<Update> {
        val lines = this::class.java.getResource(fname).readText(Charsets.UTF_8).split("\n")
        val updates = mutableListOf<Update>()

        for (line in lines) {
            val outcome = line.substringAfterLast(',')
            val input = line.substringBeforeLast(',')
            var f = Inputs()
            f = Inputs(mapOf("q" to input))
            updates.add(Update(f, outcome))
        }
        return updates
    }
}


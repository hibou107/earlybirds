import org.scalatest.{FlatSpec, Matchers}

class Tests extends FlatSpec with Matchers {
  "Algo" should "do theo job " in {
    val fixtures =
      """user1,item1,1,1476403199713
        |user1,item3,1,1476489599713
        |user2,item4,2,1476575999713""".stripMargin

    val (computed, result) = Main.compute(Const.PENALTY, Const.LOWEST_SCORE, () => fixtures.split("\n").toIterator)
    computed.userMap.size shouldBe 2
    computed.productMap.size shouldBe 3
    computed.maxTimestamp shouldBe 1476575999713L
    result((1, 2)) shouldBe 2.0
  }
}
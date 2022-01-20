package com.myself.notes._2_x

import com.myself.notes._2_x.CompanionObjectNotes.Companions.visibleToClass

import java.util.UUID

// https://www.james-willett.com/blog/scala_fp_series
// https://www.james-willett.com/blog/scala_oo_series/

// Scala Types tree dependency
// https://yilingui.xyz/2014/05/28/scala_week3_notes/

object ByNameParametersNotes extends App {

  // called by value
  //  evaluated as operator
  def calledByValue(condition: Boolean)(action: Unit) =
    if (condition) action

  calledByValue(false)(println("Always printable"))


  // called by name parameters
  //  evaluated as def rather then operator
  def calledByName(condition: Boolean)(action: => Unit) =
    if (condition) action

  calledByName(false) {

    println("Printable only when condition is true")
  }

}

object ParameterFieldNotes extends App {

  class WithParameter(parameter: Int) {

    val fromParameter = parameter
  }

  val testWithParameter = new WithParameter(1)
  //  testWithParameter.parameter - not visible this way
  assert(testWithParameter.fromParameter == 1)

  class WithField(val field: Int)

  val testWithField = new WithField(2)
  assert(testWithField.field == 2)

}

object AuxiliaryConstructorsNotes extends App {

  val expected = "withDefault"

  // we just gain different possibilities to create objects of class
  //  other then primary and defined with keyword - this
  class WithAuxiliaryConstructor(parameter: String) {

    val parameterA: String = parameter

    def this() = {

      this(expected)
    }

  }

  assert(new WithAuxiliaryConstructor().parameterA == expected)

}

object SingletonObjectNotes extends App {

  // An object is a class that has exactly one instance. It is created lazily when it is referenced, like a lazy val.
  // As a top-level value, an object is a singleton.
  object Person

  val john = Person
  val mary = Person

  // both point to the same object instance
  assert(john == mary)

}

// both the object and class are defined in the same file
object CompanionObjectNotes extends App {

  val expected = "singleton"

  object Companions {

    // both object and class can see its private members
    private val visibleToClass = "I'm visible"

    // this is just a factory method
    def apply(parameter: String): Companions =
      new Companions(parameter)

    //
    def unapply(companions: Companions): Option[String] =
      if (companions.parameter.nonEmpty) Some(companions.parameter)
      else None
  }

  class Companions(val parameter: String) {

    println(visibleToClass)
  }

  // no new keyword, Companions.apply()
  val companions = Companions(expected)

  // unapply - extractor
  assert(Companions.unapply(companions).get == expected)

  // case classes come with companion objects by default

}

object TraitsVsAbstractNotes extends App {

  trait First

  trait Second

  // we can mix-in many traits in contrast to abstract classes
  object WithMixedInTraits extends First with Second

  abstract class AbstractClass(val field: Int)

  val fromAbstractClass = new AbstractClass(1) {

    println(field)
  }
  fromAbstractClass

}

// Self-types are a way to declare that a trait must be mixed into another trait,
//  even though it doesn’t directly extend it.
// That makes the members of the dependency available without imports.
object SelfTrait extends App {

  trait Session {

    def identifier: UUID
  }

  trait User {

    this: Session =>
    def startSession: String = s"New session started: $identifier"
  }

  class Game(sessionIdentifier: UUID) extends User with Session {

    override def identifier: UUID = sessionIdentifier
  }

  println(new Game(UUID.randomUUID()).startSession)

}

// Self-type annotation in Scala is a way to express dependency between two types
// If type A depends on type B, then we cannot instantiate an object of A without providing an instance of B

// We can substitute the function call with the function body and the end result should remain the same
object ReferentialTransparency extends App {

  // a function is pure if referentially transparent
  // a side effect is what makes a function impure
  var intermediate: Int = 0

  def add(value: Int): Int = {

    intermediate += value
    intermediate
  }

  def referentiallyTransparent(left: Int, right: Int): Int = {

    left + right
  }

}

// null, Nil, Null, None, Nothing Unit
object NNotes extends App {

  // null
  //  null is the value of a reference that is not referring to any object,
  //    it can be used as a replacement for all reference types — that is, all types that extend scala.AnyRef
  //      null is the one and the only instance of type Null

  // Nil
  //  Nil is an empty singleton object list that extends the List type
  //    List[Nothing]
  assert(Nil.isEmpty)

  // prepend to the List - create new List with O(1)
  val listFromScratch = "First" :: "Second" :: Nil
  assert(listFromScratch.length == 2)

  // append to the List with `:::` combines two Lists with O(n) complexity

  // None
  //  subtype of Option

  // Nothing
  //  absolutely no value in Scala, does not have any methods or values - can be used as an return type only where function throws exception
  //    extends Any type
  //      can be used instead of any Scala type for both reference types and value types
  Unit
  // Unit - ()
  //  empty return type
  //    Unit type in Scala has one singleton value that is () - Java void does not have any value

}

object VariancesNotes extends App {

  abstract class Animal(name: String)

  case class Cat(name: String) extends Animal(name)

  case class Dog(name: String) extends Animal(name)

  // List[+A]
  //  covariance: B is subtype of A
  val list: List[Animal] = List(Dog("Husky"), Dog("German Shepherd"))

  // contravariance: A is subtype of B, [-A]

  // https://stackoverflow.com/a/4531696/5738462
  // Q[B <: A], <: - Upper Bound - means that class Q can take only any class B that is a subclass of A.
  // Q[A >: B], Q only accepts supertype of B - which is A (like abstract)
  // Q[+A] means that Q can take A-N-Y class, but if B is a subclass of A, then Q[B] is considered to be a subclass of Q[A].
  // Q[+B <: A] means that class Q can only take subclasses of A as well as propagating the subclass relationship.

}

object CaseClassNotes extends App {

  // equality depends on the equality of the case class' members
  case class SomeCC(parameter: String)

  assert(SomeCC("a") == SomeCC("a"))

  // Class Parameters are promoted to Fields
  val anything = SomeCC("anything")
  assert(anything.parameter.nonEmpty)

  // Sensible toString method
  println(SomeCC("anything"))

  // Equals and Hashcode are implemented out of the box
  println(SomeCC("value").hashCode())

  class Classic(parameter: String) {

    override def toString: String = parameter

    //  Default implementation simply check the object references of two objects to verify their equality.
    //    By default, two objects are equal if and only if they are stored in the same memory address.
    //      Equal objects must have equal hashCodes
    override def equals(obj: Any): Boolean = super.equals(obj)

    // if equals is overridden hashCode also must be overridden
    override def hashCode(): Int = super.hashCode()

  }

  println(new Classic("value"))
  println(new Classic("value").hashCode())

  // Copy method
  val newAnything = anything.copy("newAnything")
  println(newAnything)

  // case classes come with companion objects by default
  val some = SomeCC
  println(some)

  // Serializable & Extractor Patterns

  // Case objects have the same properties as Case Classes

  // Case class can not extend another case class
  //  because it would not be possible to correctly implement their equality
}

// Yielding the same output for the same inputs
object BasicFunctionsNotes extends App {

  //  Methods are just functions that can access the state of the class
  //  Every function in Scala is an object
  //  Instance of a function like class, can itself be called just like a function.
  //  apply - applying function f to its parameter p
  trait MyFunction[A, B] {
    def apply(element: A): B
  }

  val doubler = new MyFunction[Int, Int] {
    override def apply(element: Int): Int = element * 2
  }

  // we can now call double function directly with the parameter as if it were a function
  // doubler
  assert(doubler(2) == 4)

  // higher-order functions
  //  passing functions as method parameter
  def withPassedParameter(parameter: Double, function: Double => Double): Double = {

    function(parameter)
  }

  val function1_1: Function1[Double, Double] = parameter => parameter * parameter
  val function1_2: Double => Double = parameter => parameter * parameter
  val function1_3 = (parameter: Double) => parameter * parameter

  assert(withPassedParameter(10, function1_3) == 100)

  // returning function
  //  : (String, String) => String
  def urlBuilder(ssl: Boolean, domainName: String): (String, String) => String = {
    val schema = if (ssl) "https://" else "http://"
    (endpoint: String, query: String) => s"$schema$domainName/$endpoint?$query"
  }

  val domainName = "www.example.com"

  def getURL: (String, String) => String = urlBuilder(ssl = true, domainName)

  val endpoint = "users"
  val query = "id=1"

  assert(getURL(endpoint, query) == "https://www.example.com/users?id=1")

  def promotion(salaries: List[Double], promotionFunction: Double => Double): List[Double] =
    salaries.map(promotionFunction)

  def smallPromotion(salaries: List[Double]): List[Double] =
    promotion(salaries, salary => salary * 1.1)

  println(smallPromotion(List(1.0, 2.0)))

  // Function Types
  //  Function1[A, R]

  // assign an object representing the function to a variable
  // Since everything is an object in Scala f can now be treated as a reference to Function1[Int,Int] object
  val function2 = (x: Int) => x + 1
  // For example, we can call toString method inherited from Any.
  // That would have been impossible for a pure function, because functions don't have methods:
  println(function2(1).toString)

  // Every object can be treated as a function, provided it has the apply method.

  def adder: Function1[Int, Function1[Int, Int]] = new Function1[Int, Function1[Int, Int]] {

    override def apply(x: Int): Function1[Int, Int] = new Function1[Int, Int] {

      override def apply(y: Int): Int = x + y
    }
  }

  def sameAdder: (Int) => ((Int) => Int) = new ((Int) => ((Int) => Int)) {

    override def apply(x: Int): (Int) => Int = new (Int => Int) {

      override def apply(y: Int): Int = x + y
    }
  }

  // anonymous function
  val fancyAdder: Int => Int => Int = (x: Int) => (y: Int) => x + y

  // curried function
  assert(adder(1)(1) == 2)
  assert(sameAdder(1)(1) == 2)

  // Int => Int => Int - function receives Int and return Int => Int
  assert(fancyAdder(1)(1) == 2)

}

object AnonymousFunctionsNotes extends App {

  // lambda functions
  // expression that uses an anonymous function instead of a variable or value
  val intAdd: (Int, Int) => Int = (x: Int, y: Int) => x + y
  val sugarIntAdd: (Int, Int) => Int = _ + _

  assert(intAdd(1, 2) == 3)
  assert(sugarIntAdd(1, 2) == 3)

  // lambda with no parameters
  val simplyDo: () => Int = () => 1
  assert(simplyDo() == 1)

  // using curly braces with lambdas
  val stringToInt = { (str: String) =>

    str.toInt
  }

  assert(stringToInt("1") == 1)
}

// A closure is a function, whose return value depends on the value of one or more variables declared outside this function
//  but in the enclosing scope

// A partial function is a function applicable to a subset of the data it has been defined for
// It does not provide an answer for every possible input value it can be given. It provides an answer only for a subset of possible data
// PF can only have ONE parameter type
object PartialFunctionsNotes extends App {

  // of type PartialFunction[A, B]
  val divide = new PartialFunction[Double, Double] {

    def apply(x: Double) = 1 / x

    def isDefinedAt(x: Double) = x != 0.0
  }

  assert(!divide.isDefinedAt(0.0))

  // Often written using case statement
  val divide2: PartialFunction[Double, Double] = {

    case d: Double if d != 0.0 => 1 / d
  }

  assert(!divide2.isDefinedAt(0.0))

  // PF can be lifted - to Option
  val liftedDivide2: Double => Option[Double] = divide2.lift
  assert(liftedDivide2(1.0).contains(1.0))
  assert(liftedDivide2(0.0).isEmpty)

  // PF chaining - with orElse
  val chainedDivide2 = divide2.orElse[Double, Double] {

    case d: Double if d == 0.0 => Double.PositiveInfinity
  }

  assert(chainedDivide2(0.0) == Double.PositiveInfinity)

  // PF extends normal functions, because they are subtype of normal functions

  // collect method takes a partial function as input and build a new collection by applying a partial function to all elements
  //  of the collection on which the function is defined
  //    it tests isDefinedAt for each element it's given
  assert((List(0.0, 1.0, 2.0) collect {
    divide2
  }) == List(1.0, 0.5))
  assert((List(100, "text") collect { case i: Int => i }) == List(100))
}

// Eliminate repetitively passing variables into a Scala function
object PartiallyAppliedFunctions extends App {

  val sum = (a: Int, b: Int) => a + b
  val standardSum = sum(100, _)

  assert(standardSum(100) == 200)

  // Partial function application
  // Turning method to function values
  def sumMethod(x: Int)(y: Int): Int = x + y

  val liftedByCompiler = sumMethod(1) _ // 'underscore' tells compiler to ETA-EXPANSION Int => Int
  sumMethod(1)(_)
}

// Converting a function with multiple arguments into a sequence of functions that take one argument
// Each function returns another function that consumes the following argument
object CurriedFunctionsMethodsNotes extends App {

  val sum: (Int, Int) => Int = (x, y) => x + y
  assert(sum(1, 1) == 2)

  // Curried FUNCTION
  val curriedSum: Int => Int => Int = x => y => x + y
  assert(curriedSum(1)(1) == 2)

  // Int => Int = y => 1 + y
  val withOne = curriedSum(1)
  assert(withOne(1) == 2)

  // Curried METHOD
  def curriedSyntacticSugarSum(x: Int)(y: Int): Int = x + y
  assert(curriedSyntacticSugarSum(1)(1) == 2)

  // !!!
  // We can't use methods in HOF - as they are part of instances of classes
  //  methods are not instances of FunctionN function type

  // Transforming method into function is called LIFTING, or ETA-EXPANSION
  val liftedWithOne: Int => Int = curriedSyntacticSugarSum(1)
  assert(liftedWithOne(1) == 2)

  val increment: Int => Int = element => element + 1

  val list = List(1, 2, 3)
  list.map(increment) // compiler does ETA-EXPANSION to (x => increment(x))

}

object HOFNotes extends App {

  val plusOne: Int => Int = (x: Int) => x + 1

  def nTimes(f: Int => Int, n: Int, x: Int): Int = {
    if (n <= 0) x
    else nTimes(f, n - 1, f(x))
  }

  def nTimesHOF(f: Int => Int, n: Int): Int => Int = {
    if (n <= 0) (x: Int) => x
    else (x: Int) => nTimesHOF(f, n - 1)(f(x))
  }

  val appliedZeroTimes = nTimesHOF(plusOne, 0)
  assert(appliedZeroTimes(2) == 2)

}

object MapFlatMapFilterNotes extends App {

  val list = List(1, 2, 3)
  val toPair = (x: Int) => List(x, x + 1)

  // map + flatten -> flatMap
  assert(list.map(element => toPair(element)).flatten == list.flatMap(toPair))

  // two loops
  val left = List(1, 2, 3)
  val right = List("a", "b", "c")

  println(left.flatMap(number => right.map(letter => number + letter)))

}

object ForComprehensionNotes extends App {

  val numbers = List(1, 2, 3, 4)
  val chars = List('a', 'b', 'c', 'd')
  val colors = List("black", "white")

  val combinationsWithColorWithFilter =
    numbers
      .filter(_ % 2 == 0)
      .flatMap(n => chars.flatMap(c => colors.map(color => "" + c + n + "-" + color)))

  // flatMap + flatMap + map
  val forCombinations = for {
    n <- numbers
    c <- chars
    color <- colors
  } yield "" + c + n + "-" + color

  // for-comprehension uses withFilter with guards
  val left = for {
    element <- List(1, 2, 3) if element % 2 == 0
  } yield element + 1

  val right = List(1, 2, 3).withFilter(_ % 2 == 0).map(_ + 1)
  assert(left == right)

  // for-comprehension with side effects
  for {
    n <- numbers
  } println(n)

}

// Scala gives the possibility to match a pattern instead of exact value (as Java does)
// https://www.baeldung.com/scala/pattern-matching
object PatternMatchingNotes extends App {

  abstract class Shape

  case class _2D(x: Double = 0.0d, y: Double = 0.0d) extends Shape

  case class _3D(x: Double = 0.0d, y: Double = 0.0d, z: Double = 0.0d) extends Shape

  def caseClassesPatternMatchingMethod: Any => Double = (possiblyShape: Any) => {

    possiblyShape match {
      case _2D(x, y) => x * y
      case _3D(x, y, z) => x * y * z
      case _ => 0.0d
    }
  }

  assert(caseClassesPatternMatchingMethod(_2D(1.0, 2.0)) == 2.0)

  val caseClassesPatternMatchingFunction: Any => Double = {

    case _2D(x, y) => x * y
    case _3D(x, y, z) => x * y * z
    case _ => 0.0d
  }

  assert(caseClassesPatternMatchingFunction(_2D(1.0, 2.0)) == 2.0)

  // Variable bindings
  def binderPatternMatching: Any => String = {

    case binding@_2D(_, _) => s"X is ${binding.x}"
    case _3D(x, y, z) => s"Sphere of $x, $y, $z"
    case _ => "unknown animal"
  }

  // Variable bindings with part matching
  def binderPatternWithPartMatch: Any => Double = {
    case _2D(x@10.0d, _) => x * 10.0d
    case _ => 0.0d
  }

  assert(binderPatternWithPartMatch(_2D(10.0d, -1.0d)) == 100.0d)

  // Pattern guards as curried function
  def patternGuards: Any => Double => Double = (possiblyShape: Any) => (minimumDimensionValue: Double) => {
    possiblyShape match {
      case _2d: _2D if _2d.x < minimumDimensionValue => minimumDimensionValue
      case _ => 0.0d
    }
  }

  assert(patternGuards(_2D(-100.0d, 100.0d))(100.0d) == 100.0d)

  // sealed case classes - default case is not mandatory

  // Extractors - Objects
  class Person(val name: String, val age: Int)

  object Person {

    def apply(name: String, age: Int): Person = new Person(name, age)

    def unapply(person: Person): Option[(String, Int)] = {

      Some(person.name, person.age)
    }

  }

  def extractor: Any => Option[(String, Int)] = {
    case Person(name, age) => Some((name, age))
    case _ => None
  }

  assert(extractor(Person("John", 30)).contains(("John", 30)))

}

object CollectionsNotes extends App {

  // https://docs.scala-lang.org/overviews/collections/performance-characteristics.html
  // sequence-like collections/sets/maps
}

// Problem with recursion is that deep recursion can blow up the stack if we are not careful - java.lang.StackOverflowError
object RecursionNotes extends App {

  def sum(num: Int): Int = {

    if (num == 1)
      1
    else
      sum(num - 1) + num
  }

  assert(sum(3) == 6)

  // A recursive function is said to be tail recursive if the recursive call is the last thing done by the function
  // There is no need to keep record of the previous state.

  import scala.annotation.tailrec

  @tailrec
  def factorialAcc(acc: Int, n: Int): Int = {
    if (n <= 1)
      acc
    else
      factorialAcc(n * acc, n - 1)
  }

  assert(factorialAcc(1, 3) == 6)
}

// Recalculate rather than serialize logic
// Field of the object will be calculated AT MOST ONCE per deserialization
object TransientLazyValNotes extends App {

  class SomeClass extends Serializable {
    @transient lazy val field: String = {
      println("Calculate field")

      "value"
    }
  }

  val instance = new SomeClass()
  println("Before serialization")
  instance.field
  instance.field // calculated only once

  // Serialize
  import java.io._
  val byteOutputStream = new ByteArrayOutputStream
  val outputStream = new ObjectOutputStream(byteOutputStream)
  outputStream.writeObject(instance)
  val bytes = byteOutputStream.toByteArray

  // Deserialize
  val byteInputStream = new ByteArrayInputStream(bytes)
  val inputStream = new ObjectInputStream(byteInputStream)
  val instance2 = inputStream.readObject.asInstanceOf[SomeClass]

  println("After deserialization")
  instance2.field
  instance2.field

}

// Concurency
//  http://twitter.github.io/scala_school/concurrency.html

// Monads are a kind of abstract types which have some fundamental operations
object MonadsNotes extends App {

  trait MonadTemplate[A] {
    def apply(value: A): MonadTemplate[A]

    def flatMap[B](f: A => MonadTemplate[B]): MonadTemplate[B]
  }

  // List, Option, Try, Future, Stream, Set are all monads
  // Operations must follow some monad laws
  // - left-identity
  // - right-identity
  // - associativity
}
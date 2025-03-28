# scala threadmanager (`1.0.0`)

While working with the command line and parallelizing execution I encountered the same code patterns over
and over again. This library is my attempt at factorizing those patterns into one neat dependency.


## Install using sbt

You need to add the dependency
```
    "de.halcony"                 %% "scala-threadmanager"                % "(version)"
```

as well as the resolver

```
resolvers ++= Seq(
    "Sonatype OSS Snapshots" at "https://s01.oss.sonatype.org/content/repositories/public",
)
```


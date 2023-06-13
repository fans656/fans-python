from fans.bunch import bunch


quantix = bunch(
    port = 6562,
)

stome = bunch(
    cloud = bunch(
        port = 6563, # cloud server port
    ),
    desktop = bunch(
        port = 6660, # desktop backend listening port
    ),
)

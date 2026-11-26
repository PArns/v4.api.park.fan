### Fixed — Parky's "the way there" card no longer fails when the model leaves out the park

`show_card` kind `navigate` threw "needs park and to" when `park` was missing, so a visitor who asked for
the nearest toilet got "Die Wegbeschreibung zeige ich dir hier:" and no card (two failed calls in one
chat). The park is now taken from the toilet's or drinking water's own id, else from the park the
visitor stands in; only when neither says, the error names what to pass. The tool description says
`park` should still be given.

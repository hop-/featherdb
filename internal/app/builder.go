package app

type Builder struct {
	// TODO: add fields for configuration
}

func NewBuilder() *Builder {
	return &Builder{}
}

func (b *Builder) Build() (*App, error) {
	// TODO: implement the build logic
	return &App{}, nil
}

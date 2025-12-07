package app

type App struct {
}

func (a *App) Start() error {
	err := a.init()
	if err != nil {
		return err
	}

	// TODO: implement the start logic

	return nil
}

func (a *App) Stop() error {
	// TODO: implement the stop logic
	return nil
}

func (a *App) init() error {
	// TODO: implement the initialization logic
	return nil
}

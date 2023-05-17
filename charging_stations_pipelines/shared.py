def reject_if(test: bool, error_message: str = ""):
    if test:
        raise RuntimeError(error_message)

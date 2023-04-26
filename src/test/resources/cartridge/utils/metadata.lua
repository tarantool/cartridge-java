local function get_router_name()
    return string.sub(box.cfg.custom_proc_title, 9)
end

return {
    get_router_name = get_router_name
}
